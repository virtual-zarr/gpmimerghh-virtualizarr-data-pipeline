import os
import tempfile
from datetime import datetime, timedelta

import icechunk
import numpy as np
import xarray as xr
from icechunk import Repository, Session
import zarr

from typing import TYPE_CHECKING
from itertools import islice

#if TYPE_CHECKING:
from obspec_utils.registry import ObjectStoreRegistry

CHUNK_DIR = os.path.realpath(tempfile.gettempdir())
CHUNK_DIRECTORY_URL_PREFIX = f"file://{CHUNK_DIR}/"

from . import helpers

# Coord chunk size = one year of half-hours.
TIME_CHUNK = 48 * 365  # 17_520

def _native_chunks(var) -> tuple:
    """Best-effort chunk-shape lookup for a virtualizarr-loaded variable."""
    enc_chunks = var.encoding.get("chunks") or var.encoding.get("preferred_chunks")
    if enc_chunks:
        return tuple(enc_chunks)
    if hasattr(var.data, "chunks") and var.data.chunks is not None:
        return tuple(var.data.chunks)
    raise ValueError("Couldn't determine native chunk shape for variable")

def _is_initialized(repo: icechunk.Repository, n_time: int = helpers.N_TIME) -> bool:
    """Has the template commit been made yet?
    """

    # A freshly created Icechunk repo has exactly one ancestor (the root /
    # "Repository initialized" commit). Once the full empty arrays have been committed
    # there are at least two ancestors. We use ``islice(..., 2)`` so we never
    # walk the full history.    
    # len(list(islice(repo.ancestry(branch="main"), 2))) > 1
    session = repo.readonly_session("main")
    if len(list(islice(repo.ancestry(branch="main"), 2))) > 1:
        root = zarr.open_group(store=session.store, mode="r")
        return root['time'].shape == (n_time,)
    else:
        return False

def _time_index_for(t: datetime) -> int:
    """Half-hour offset from the cube's epoch (1998-01-01 00:00:00 UTC)."""
    delta = t - helpers.T0
    if delta < timedelta(0):
        raise ValueError(f"{t!r} is before the cube epoch {helpers.T0!r}")
    seconds = int(delta.total_seconds())
    if seconds % 1800 != 0:
        raise ValueError(f"{t!r} is not aligned to a 30-minute boundary")
    return seconds // 1800

def _timestamp_from_url(file_url: str) -> datetime:
    """Parse the start timestamp out of a 3B-HHR... filename.

    Example filename:
      3B-HHR.MS.MRG.3IMERG.20250930-S233000-E235959.1410.V07B.HDF5
    """
    filename = file_url.rsplit("/", 1)[-1]
    # token "20250930-S233000" → date + start-of-half-hour
    date_part, start_part = filename.split(".")[4].split("-")[:2]
    return datetime.strptime(date_part + start_part[1:], "%Y%m%d%H%M%S")

class Processor:
    region: str
    prefix: str
    bucket: str | None
    storage: icechunk.Storage | None

    def __init__(
        self,
        region: str = 'us-west-2',
        bucket: str | None = None,
        prefix: str = "gpmimerg_hh_07",
    ) -> None:
        self.region = region
        self.bucket = bucket or os.getenv("ICECHUNK_BUCKET") or None
        self.prefix = (prefix or os.getenv("ICECHUNK_PREFIX", "gpmimerg_hh_07")).strip("/")
        self.storage = self._get_storage()

    def _get_storage(self) -> icechunk.Storage:
        if not self.bucket:
            return icechunk.local_filesystem_storage(path=self.prefix)

        return icechunk.s3_storage(
            bucket=self.bucket,
            prefix=self.prefix or None,
            region=self.region,
            from_env=True,
        )

    def initialize_repo(
        self,
        repo: icechunk.Repository | None = None,
        sample: xr.Dataset | None = None,
        *,
        n_time: int = helpers.N_TIME,
        t0: datetime = helpers.T0,
        time_chunk: int = TIME_CHUNK,
    ) -> icechunk.Repository:
        """Open or create the repo and, on first call only, write the coordinate
        template.

        Returns the open ``Repository``. Safe to call repeatedly — subsequent
        calls just return the existing repo.

        Parameters
        ----------
        repo:
            Pre-opened repository. If ``None``, ``helpers.open_or_create_repo()``
            is called with default (production) arguments.
        sample:
            Sample granule dataset, as returned by ``helpers.open_vds_with_coords``.
            Used to pull coord values, attrs, and per-data-variable chunk shapes /
            fill values / codecs. If ``None``, one is opened from S3 for ``t0``.
        n_time:
            Number of timesteps in the target cube. Defaults to ``N_TIME``.
        t0:
            Start of the target time axis. Defaults to ``T0`` (1998-01-01).
        time_chunk:
            Chunk size along the time axis for the native ``time`` coord.
        """
        if repo is None:
            repo = helpers.open_or_create_repo(storage=self.storage, save_config=True)

        if _is_initialized(repo):
            print("Repo already initialized; skipping template write.")
            return repo

        time = np.array(
            [t0 + i * timedelta(minutes=30) for i in range(n_time)],
            dtype="datetime64[ns]",
        )

        if sample is None:
            # Pull metadata from one sample file. open_vds_with_coords loads
            # coords + bounds natively (via loadable_variables) so we can read
            # their values here.
            sample = helpers.open_vds_with_coords(helpers.url_for(t0))
        nlon = sample.sizes["lon"]
        nlat = sample.sizes["lat"]

        # Write everything directly via zarr
        session = repo.writable_session("main")
        root = zarr.open_group(store=session.store, mode="w")

        # time: int64 nanoseconds since epoch; CF attrs let xarray decode on read
        root.create_array(
            "time",
            shape=(n_time,),
            dtype="int64",
            chunks=(time_chunk,),
            dimension_names=("time",),
        )
        root["time"][:] = time.view("int64")
        root["time"].attrs.update({
            "units": "nanoseconds since 1970-01-01",
            "calendar": "proleptic_gregorian",
        })

        # lon / lat — small enough to write in one shot
        lon_data = sample.lon.values
        root.create_array("lon", shape=lon_data.shape, dtype=lon_data.dtype,
                        chunks=(nlon,), dimension_names=("lon",))
        root["lon"][:] = lon_data
        root["lon"].attrs.update(dict(sample.lon.attrs))

        lat_data = sample.lat.values
        root.create_array("lat", shape=lat_data.shape, dtype=lat_data.dtype,
                        chunks=(nlat,), dimension_names=("lat",))
        root["lat"][:] = lat_data
        root["lat"].attrs.update(dict(sample.lat.attrs))

        # Global attributes
        root.attrs.update(dict(sample.attrs))

        # Data variables — metadata + fill value only, no chunk data written.
        # Bounds (time_bnds, lon_bnds, lat_bnds) are 2-D and may appear in
        # data_vars; skip anything that isn't a 3-D (time, lon, lat) array.
        for name, var in sample.data_vars.items():
            src_chunks = _native_chunks(var)
            if len(src_chunks) != 3:
                continue
            # first chunk dimension is time, which always has a chunk size of 1 since GPM IMERG HH files only store 1 time step.
            chunks = (1, src_chunks[1], src_chunks[2])
            arr = root.create_array(
                name=name,
                shape=(n_time, nlon, nlat),
                chunks=chunks,
                dtype=var.dtype,
                fill_value=var.data.metadata.fill_value,
                dimension_names=("time", "lon", "lat"),
                serializer=var.data.metadata.codecs[0],
                compressors=var.data.metadata.codecs[1:],
            )
            arr.attrs.update(dict(var.attrs))
        session.commit("init: full shape + coords + native chunk grids")
        print("Committed initial template.")
        return repo

    def initialize_session(self, repo: Repository) -> Session:
        session = repo.writable_session("main")
        return session

    def process_file(
        self,
        file_url: str,
        session: Session,
        *,
        t: datetime | None = None,
        registry: ObjectStoreRegistry | None = None,
    ) -> bool:
        """Write one half-hour granule into its region of the store.

        Parameters
        ----------
        file_url : str
            Full ``s3://`` URL of the source HDF5 granule.
        session : icechunk.Session
            Writable session. The caller is responsible for committing — this
            function only stages writes, matching the VDP processor contract.
        t : datetime, optional
            The granule's timestamp. If omitted it is parsed from ``file_url``.
        registry : ObjectStoreRegistry, optional
            Forwarded to ``helpers.open_vds_data_only``. Production callers leave
            this ``None`` (default GES DISC S3 registry); tests pass a
            ``LocalStore``-backed registry pointing at a fixture file.
        """
        if t is None:
            t = _timestamp_from_url(file_url)
        time_idx = _time_index_for(t)
        vds = helpers.open_vds_data_only(file_url, registry=registry)
        vds.vz.to_icechunk(
            session.store,
            region={"time": slice(time_idx, time_idx + 1)},
        )
        return True

    def commit_processed_files(self, session: Session) -> str:
        snapshot = session.commit(message=f"Append to {session.snapshot_id}")
        return str(snapshot)

    def garbage_collect(self, expiry_time: datetime, repo: icechunk.Repository | None = None) -> icechunk.GCSummary:
        repo.expire_snapshots(older_than=expiry_time)
        gcs = repo.garbage_collect(delete_object_older_than=expiry_time)
        return gcs
