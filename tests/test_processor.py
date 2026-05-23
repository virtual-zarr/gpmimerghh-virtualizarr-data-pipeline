from datetime import datetime, timedelta, timezone
from pathlib import Path

import icechunk
import numpy as np
from icechunk import Repository, Session
from obspec_utils.registry import ObjectStoreRegistry
from virtualizarr_processor import helpers
from virtualizarr_processor.processor import Processor
from virtualizarr_processor.typing import VirtualizarrProcessor


def protocol_type_check(processor: VirtualizarrProcessor) -> None:
    assert processor


def test_follows_protocol() -> None:
    processor = Processor()
    protocol_type_check(processor=processor)


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


def test_garbage_collect(initialized_repo: icechunk.Repository) -> None:
    processor = Processor()
    expiry_time = datetime.now(timezone.utc) - timedelta(days=2)
    gcs = processor.garbage_collect(expiry_time=expiry_time, repo=initialized_repo)
    assert isinstance(gcs, icechunk.GCSummary)
