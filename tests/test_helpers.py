"""Unit tests for cached credential/registry construction in ``helpers``.

These guard the fix for bursts of Earthdata ``s3credentials`` requests: building
a fresh credential provider / S3 store per granule defeated obstore's credential
cache and intermittently failed with ``UnauthenticatedError``. The provider and
registry are now memoized per process, so they must be constructed exactly once.
"""

from __future__ import annotations

from collections.abc import Iterator
from types import SimpleNamespace

import pytest
from virtualizarr_processor import helpers


@pytest.fixture(autouse=True)
def _clear_caches() -> Iterator[None]:
    """Isolate each test from cached state populated by others."""
    helpers._credential_provider.cache_clear()
    helpers._default_s3_registry.cache_clear()
    yield
    helpers._credential_provider.cache_clear()
    helpers._default_s3_registry.cache_clear()


def test_credential_provider_is_cached(monkeypatch: pytest.MonkeyPatch) -> None:
    constructed: list[str] = []

    class _DummyProvider:
        def __init__(self, url: str) -> None:
            constructed.append(url)

    monkeypatch.setattr(helpers, "_load_earthdata_credentials", lambda: None)
    monkeypatch.setattr(helpers, "NasaEarthdataCredentialProvider", _DummyProvider)

    first = helpers._credential_provider()
    second = helpers._credential_provider()

    assert first is second
    assert constructed == [helpers.CREDENTIALS_URL]  # built once despite two calls


def test_default_s3_registry_is_cached(monkeypatch: pytest.MonkeyPatch) -> None:
    made: list[object] = []

    monkeypatch.setattr(helpers, "_credential_provider", lambda: object())
    monkeypatch.setattr(
        helpers, "S3Store", SimpleNamespace(from_url=lambda *a, **k: object())
    )

    def _fake_registry(mapping: dict) -> object:
        obj = object()
        made.append(obj)
        return obj

    monkeypatch.setattr(helpers, "ObjectStoreRegistry", _fake_registry)

    first = helpers._default_s3_registry()
    second = helpers._default_s3_registry()

    assert first is second
    assert len(made) == 1  # registry (and its store) constructed once
