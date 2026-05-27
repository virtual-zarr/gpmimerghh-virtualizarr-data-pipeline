import json
import os
from datetime import datetime, timedelta
from typing import Dict

import boto3
import icechunk
import xarray as xr
from obspec_utils.registry import ObjectStoreRegistry
from obstore.auth.earthdata import NasaEarthdataCredentialProvider
from obstore.store import S3Store
from virtualizarr import open_virtual_dataset
from virtualizarr.parsers import HDFParser


def _load_earthdata_credentials() -> None:
    """Fetch Earthdata credentials from Secrets Manager and set as env vars."""
    if os.environ.get("EARTHDATA_USERNAME") and os.environ.get("EARTHDATA_PASSWORD"):
        return
    secret_arn = os.environ.get("EARTHDATA_SECRET_ARN")
    if not secret_arn:
        return
    client = boto3.client("secretsmanager")
    response = client.get_secret_value(SecretId=secret_arn)
    creds = json.loads(response["SecretString"])
    os.environ["EARTHDATA_USERNAME"], os.environ["EARTHDATA_PASSWORD"] = (
        creds["username"],
        creds["password"],
    )


# Auxiliary variables that we never want in the analysis-ready cube. The
# `Grid` group plus these dimension/bounds variables are dropped on *every*
# read.
AUX_DROP_VARIABLES = ["Intermediate", "nv", "lonv", "latv"]

# Coordinate + bounds variables. During repo + dataset initialization these are loaded
# natively so we can extract their values; in during region writing, these
# are dropped because they're already written in the store.
COORD_VARIABLES = ["time", "lon", "lat", "time_bnds", "lon_bnds", "lat_bnds"]

BUCKET = "gesdisc-cumulus-prod-protected"
STORE_PREFIX = f"s3://{BUCKET}/GPM_L3/GPM_3IMERGHH.07/"
FILE_PATH = "2025/273/3B-HHR.MS.MRG.3IMERG.20250930-S233000-E235959.1410.V07B.HDF5"
EXAMPLE_LINK = f"{STORE_PREFIX}/{FILE_PATH}"
CREDENTIALS_URL = "https://data.gesdisc.earthdata.nasa.gov/s3credentials"

# TIME VARS
T0 = datetime(1998, 1, 1)
T_MINUS_1 = datetime(2025, 10, 1)
N_TIME = (T_MINUS_1 - T0).days * 48


def url_for(t: datetime) -> str:
    end = t + timedelta(minutes=29, seconds=59)
    midnight = datetime(t.year, t.month, t.day)
    minutes_since = (t - midnight) // timedelta(minutes=1)
    name = (
        "3B-HHR.MS.MRG.3IMERG."
        + t.strftime("%Y%m%d")
        + "-S"
        + t.strftime("%H%M%S")
        + "-E"
        + end.strftime("%H%M%S")
        + f".{minutes_since:04d}.V07B.HDF5"
    )
    return f"{STORE_PREFIX}/{t.year:04d}/{t.strftime('%j')}/{name}"


def _credential_provider() -> NasaEarthdataCredentialProvider:
    _load_earthdata_credentials()
    return NasaEarthdataCredentialProvider(CREDENTIALS_URL)


def _default_s3_registry(data_url: str) -> ObjectStoreRegistry:
    """Build the production GES DISC S3 registry for ``data_url``."""
    cp = _credential_provider()
    store = S3Store.from_url(STORE_PREFIX, credential_provider=cp)
    return ObjectStoreRegistry({f"s3://{BUCKET}": store})


def _open_vds(
    data_url: str,
    *,
    drop_variables: list[str],
    loadable_variables: list[str],
    registry: ObjectStoreRegistry | None = None,
) -> xr.Dataset:
    """
    Open a granule with explicit drop_variables / loadable_variables.

    All public openers below are thin wrappers around this so the parser /
    registry / credential setup lives in exactly one place. Tests can pass
    their own ``registry`` (e.g. one wrapping a ``LocalStore``) to read a
    fixture file without touching S3.
    """
    if registry is None:
        registry = _default_s3_registry(data_url)
    parser = HDFParser(group="Grid", drop_variables=drop_variables)
    return open_virtual_dataset(
        url=data_url,
        parser=parser,
        registry=registry,
        loadable_variables=loadable_variables,
    )


def open_vds_with_coords(
    data_url: str,
    *,
    registry: ObjectStoreRegistry | None = None,
) -> xr.Dataset:
    """
    Returns a vds with coords + bounds loaded.

    Use this when you need to read the coordinate values — e.g. to extract
    time,lon and lat as an empty data cube. Coords come back as materialized
    numpy arrays; data variables come back as VirtualiZarr ManifestArrays.
    """
    return _open_vds(
        data_url,
        drop_variables=AUX_DROP_VARIABLES,
        loadable_variables=COORD_VARIABLES,
        registry=registry,
    )


def open_vds_data_only(
    data_url: str,
    *,
    registry: ObjectStoreRegistry | None = None,
) -> xr.Dataset:
    """
    Returns a vds with the 4 data variables and no coordinates.
    Useful for region-writing.

    Every coordinate and bound variable is added to `drop_variables` so the
    HDF parser never reads them, and `loadable_variables` is empty so nothing
    gets materialised. The result can be written straight into
    ``region={"time": slice(t, t+1)}`` without any post-hoc ``drop_vars``.
    """
    return _open_vds(
        data_url,
        drop_variables=AUX_DROP_VARIABLES + COORD_VARIABLES,
        loadable_variables=[],
        registry=registry,
    )


def get_icechunk_creds() -> icechunk.S3StaticCredentials:
    """Get refreshable earthdata credentials for icechunk."""
    creds = _credential_provider()()
    return icechunk.S3StaticCredentials(
        access_key_id=creds["access_key_id"],
        secret_access_key=creds["secret_access_key"],
        session_token=creds["token"],
    )


def get_container_credentials() -> icechunk.AnyCredential:
    """Get container credentials for icechunk."""
    return icechunk.containers_credentials(
        {
            STORE_PREFIX: icechunk.s3_refreshable_credentials(
                get_credentials=get_icechunk_creds
            )
        }
    )


def open_or_create_repo(
    *,
    storage: "icechunk.Storage",
    manifest_split_size: int = 48 * 365,
    virtual_chunk_url: str | None = None,
    virtual_chunk_store: "icechunk.ObjectStoreConfig | None" = None,
    virtual_chunk_credentials: "Dict[str, icechunk.AnyCredential] | None" = None,
) -> icechunk.Repository:
    """Open or create the GPM_3IMERGHH icechunk repo.
    Parameters
    ----------
    storage:
        Icechunk ``Storage`` for the repo itself. If ``None`` defaults to
        ``local_filesystem_storage(path=storage_path)``.
    manifest_split_size:
        Number of timesteps per manifest shard.
    virtual_chunk_url:
        URL prefix for virtual chunk references. Defaults to the GES DISC
        IMERG prefix derived from ``EXAMPLE_LINK``.
    virtual_chunk_store:
        Icechunk ``ObjectStoreConfig`` describing how to read virtual chunks.
        Defaults to an S3 store in ``us-west-2``.
    virtual_chunk_credentials:
        Credentials map for the virtual chunk container. Defaults to
        refreshable Earthdata creds for the GES DISC prefix. Pass ``{}`` or
        an empty dict-like for tests that don't need credentials.
    """

    # Fallbacks
    if virtual_chunk_url is None:
        virtual_chunk_url = STORE_PREFIX

    if virtual_chunk_store is None:
        virtual_chunk_store = icechunk.s3_store(region="us-west-2")

    if virtual_chunk_credentials is None:
        virtual_chunk_credentials = get_container_credentials()

    try:
        repo = icechunk.Repository.open(
            storage=storage,
            authorize_virtual_chunk_access=virtual_chunk_credentials,
        )
    except icechunk.IcechunkError:
        config = icechunk.RepositoryConfig.default()
        time_split_size = {
            icechunk.config.ManifestSplitDimCondition.DimensionName(
                "time"
            ): manifest_split_size
        }
        config.manifest = icechunk.ManifestConfig(
            splitting=icechunk.ManifestSplittingConfig.from_dict(
                {
                    icechunk.config.ManifestSplitCondition.name_matches(
                        "precipitation"
                    ): time_split_size,
                    icechunk.config.ManifestSplitCondition.name_matches(
                        "randomError"
                    ): time_split_size,
                    icechunk.config.ManifestSplitCondition.name_matches(
                        "precipitationQualityIndex"
                    ): time_split_size,
                    icechunk.config.ManifestSplitCondition.name_matches(
                        "probabilityLiquidPrecipitation"
                    ): time_split_size,
                }
            ),
            preload=icechunk.ManifestPreloadConfig(max_total_refs=0),
        )
        config.set_virtual_chunk_container(
            icechunk.VirtualChunkContainer(virtual_chunk_url, virtual_chunk_store)
        )
        repo = icechunk.Repository.create(
            storage=storage,
            config=config,
            authorize_virtual_chunk_access=virtual_chunk_credentials,
        )
        repo.save_config()
    return repo
