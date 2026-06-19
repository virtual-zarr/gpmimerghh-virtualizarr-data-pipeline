import os
import tempfile
from pathlib import Path

import icechunk
import numpy as np
import obstore
import pytest
import xarray as xr
from hdf5_fixtures import _build_fixture
from obspec_utils.registry import ObjectStoreRegistry
from obstore.store import LocalStore
from virtualizarr.manifests import ChunkManifest, ManifestArray
from virtualizarr_processor import helpers
from virtualizarr_processor.processor import Processor
from zarr.codecs import BytesCodec
from zarr.core.dtype import parse_data_type
from zarr.core.metadata import ArrayV3Metadata

CHUNK_DIR = os.path.realpath(tempfile.gettempdir())
CHUNK_DIRECTORY_URL_PREFIX = f"file://{CHUNK_DIR}/"

# Small cube dimensions kept tiny so tests finish quickly.
NLON = 12
NLAT = 6
N_TIME = 48


def fake_vds(date: str) -> xr.Dataset:
    filepath = f"{CHUNK_DIR}/data_chunk"
    store = obstore.store.LocalStore()
    arr = np.repeat([[1, 2]], 3, axis=1)
    shape = arr.shape
    dtype = arr.dtype
    buf = arr.tobytes()
    obstore.put(
        store,
        filepath,
        buf,
    )
    manifest = ChunkManifest(
        {"0.0": {"path": filepath, "offset": 0, "length": len(buf)}}
    )
    zdtype = parse_data_type(dtype, zarr_format=3)
    metadata = ArrayV3Metadata(
        shape=shape,
        data_type=zdtype,
        chunk_grid={
            "name": "regular",
            "configuration": {"chunk_shape": shape},
        },
        chunk_key_encoding={"name": "default"},
        fill_value=zdtype.default_scalar(),
        codecs=[BytesCodec()],
        attributes={},
        dimension_names=("y", "x"),
        storage_transformers=None,
    )
    ma = ManifestArray(
        chunkmanifest=manifest,
        metadata=metadata,
    )
    foo = xr.Variable(data=ma, dims=["y", "x"], encoding={"scale_factor": 2})
    vds = xr.Dataset(
        {"foo": foo},
        coords={
            "time": ("time", [np.datetime64(date)])  # Single time point
        },
    )
    return vds


def create_repo() -> icechunk.Repository:
    chunk_store = icechunk.local_filesystem_store(CHUNK_DIR)
    storage = icechunk.in_memory_storage()
    config = icechunk.RepositoryConfig.default()
    config.set_virtual_chunk_container(
        icechunk.VirtualChunkContainer(CHUNK_DIRECTORY_URL_PREFIX, chunk_store)
    )
    repo = icechunk.Repository.open_or_create(
        storage=storage,
        config=config,
        authorize_virtual_chunk_access={CHUNK_DIRECTORY_URL_PREFIX: None},
    )
    return repo


def create_session() -> icechunk.Session:
    repo = create_repo()
    session = repo.writable_session("main")
    vds = fake_vds("2024-01-01")
    vds.vz.to_icechunk(session.store, validate_containers=False)
    return session


@pytest.fixture(scope="function")
def icechunk_repo() -> icechunk.Repository:
    return create_repo()


@pytest.fixture(scope="function")
def icechunk_session() -> icechunk.Session:
    return create_session()


@pytest.fixture
def local_registry(tmp_path: Path) -> ObjectStoreRegistry:
    """Resolves file:// URLs under ``tmp_path`` to a LocalStore."""
    return ObjectStoreRegistry({f"file://{tmp_path}": LocalStore()})


@pytest.fixture
def fixture(tmp_path: Path) -> tuple[Path, dict[str, np.ndarray]]:
    """Tiny on-disk HDF5 file + dict of expected per-variable data."""
    path = tmp_path / "fixture.HDF5"
    expected = _build_fixture(path, nlon=NLON, nlat=NLAT, populate_data=True)
    return path, expected


@pytest.fixture
def initialized_repo(
    tmp_path: Path,
    fixture: tuple[Path, dict[str, np.ndarray]],
    local_registry: ObjectStoreRegistry,
) -> icechunk.Repository:
    """
    Local icechunk repo with the Stage-0 template committed, ready for region writes.
    """
    fixture_file, _ = fixture
    virtual_chunk_url = f"file://{tmp_path}/"
    repo = helpers.open_or_create_repo(
        storage=icechunk.local_filesystem_storage(path=str(tmp_path / "repo")),
        manifest_split_size=N_TIME,
        virtual_chunk_url=virtual_chunk_url,
        virtual_chunk_store=icechunk.local_filesystem_store(str(tmp_path)),
        virtual_chunk_credentials={virtual_chunk_url: None},
    )
    sample = helpers.open_vds_with_coords(
        f"file://{fixture_file}",
        registry=local_registry,
    )
    Processor().initialize_repo(
        repo=repo,
        sample=sample,
        n_time=N_TIME,
        t0=helpers.T0,
        time_chunk=N_TIME,
    )
    return repo
