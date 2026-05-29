from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import MagicMock

import icechunk
import numpy as np
import pytest
from icechunk import Repository, Session
from obspec_utils.registry import ObjectStoreRegistry
from virtualizarr_processor import helpers
from virtualizarr_processor.processor import Processor
from virtualizarr_processor.typing import VirtualizarrProcessor


def _conflict() -> icechunk.ConflictError:
    return icechunk.ConflictError("expected", "actual")


def _accepts_protocol(_: VirtualizarrProcessor) -> None:
    """
    Static-check helper: type-checkers verify the argument conforms to the protocol.
    """


def test_follows_protocol() -> None:
    processor = Processor()
    _accepts_protocol(processor)
    assert isinstance(processor, VirtualizarrProcessor)


def test_initialize_repo(initialized_repo: icechunk.Repository) -> None:
    assert isinstance(initialized_repo, Repository)


def test_process_file(
    initialized_repo: icechunk.Repository,
    fixture: tuple[Path, dict[str, np.ndarray]],
    local_registry: ObjectStoreRegistry,
) -> None:
    processor = Processor()
    fixture_file, _ = fixture
    session = initialized_repo.writable_session("main")
    result = processor.process_file(
        file_url=f"file://{fixture_file}",
        session=session,
        t=helpers.T0,
        registry=local_registry,
    )
    assert result


def test_commit_processed_files(icechunk_session: Session) -> None:
    processor = Processor()
    snapshot = processor.commit_processed_files(session=icechunk_session)
    assert isinstance(snapshot, str)


def test_commit_retries_then_succeeds_after_rebase() -> None:
    """A racing commit conflicts, rebases, and succeeds on the next attempt."""
    session = MagicMock(spec=Session)
    session.snapshot_id = "snap"
    session.commit.side_effect = [_conflict(), _conflict(), "snap-final"]

    snapshot = Processor().commit_processed_files(session, base_backoff=0.0)

    assert snapshot == "snap-final"
    assert session.commit.call_count == 3
    assert session.rebase.call_count == 2  # one rebase before each retry
    solver = session.rebase.call_args.args[0]
    assert isinstance(solver, icechunk.ConflictDetector)


def test_commit_reraises_after_exhausting_attempts() -> None:
    """Persistent conflicts exhaust the budget and re-raise so SQS redelivers."""
    session = MagicMock(spec=Session)
    session.snapshot_id = "snap"
    session.commit.side_effect = _conflict()

    with pytest.raises(icechunk.ConflictError):
        Processor().commit_processed_files(session, max_attempts=3, base_backoff=0.0)

    assert session.commit.call_count == 3
    assert session.rebase.call_count == 2  # no rebase after the final failed commit


def test_commit_propagates_rebase_failure() -> None:
    """A genuine (non-disjoint) conflict surfaces instead of retrying forever."""
    session = MagicMock(spec=Session)
    session.snapshot_id = "snap"
    session.commit.side_effect = _conflict()
    session.rebase.side_effect = icechunk.RebaseFailedError("snap", [])

    with pytest.raises(icechunk.RebaseFailedError):
        Processor().commit_processed_files(session, base_backoff=0.0)

    assert session.commit.call_count == 1  # stop at the first unresolved rebase


def test_garbage_collect(initialized_repo: icechunk.Repository) -> None:
    processor = Processor()
    expiry_time = datetime.now(timezone.utc) - timedelta(days=2)
    gcs = processor.garbage_collect(expiry_time=expiry_time, repo=initialized_repo)
    assert isinstance(gcs, icechunk.GCSummary)
