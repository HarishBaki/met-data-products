# %%
import numpy as np
import pandas as pd
import xarray as xr
import dask
import os, sys
from pathlib import Path
import glob
import zarr
from joblib import Parallel, delayed
import os
import dask.array as da
import os, sys, time, glob, re
from tqdm import tqdm
import argparse
import yaml

BOOTSTRAP_ROOT = Path(__file__).resolve().parents[1]
if str(BOOTSTRAP_ROOT) not in sys.path:
    sys.path.insert(0, str(BOOTSTRAP_ROOT))

from repo_utils import load_region_grid, load_region_vars
from data_utils.zarr_io import (
    apply_var_attrs,
    ensure_store,
    get_slurm_cpus,
    has_missing_data,
    open_zarr_safe,
    write_region,
)

# Not region-specific -- the raw URMA archive covers the whole native grid
# regardless of which region's crop is being processed.
data_source_dir = '/network/rit/lab/basulab/RAW_DATA/URMA'

# %%
"""
Internal variable naming convention (cfgrib-normalized):

    si10   -> 10 m wind speed
    i10fg  -> 10 m wind gust
    t2m    -> 2 m air temperature
    sp     -> surface pressure
    d2m    -> 2 m dew point temperature
    u10    -> 10 m eastward wind
    v10    -> 10 m northward wind
    sh2    -> 2 m specific humidity
    wdir10 -> 10 m wind direction
    tp     -> total precipitation (hourly accumulated)
"""
GRIB_SHORTNAME = {
    "si10": "10si",
    "i10fg": "i10fg",
    "t2m": "2t",
    "sp": "sp",
    "d2m": "2d",
    "u10": "10u",
    "v10": "10v",
    "sh2": "2sh",
    "wdir10": "10wdir",
    "tp": "tp",
}

ALL_VARS = list(GRIB_SHORTNAME.keys())

# cfgrib may not always set units attrs; provide known source units as fallback.
# URMA data is already in SI (sp: Pa, tp: kg m**-2) so most need only notation normalization.
_GRIB_SOURCE_UNITS: dict = {
    "si10": "m s-1",
    "i10fg": "m s-1",
    "t2m": "K",
    "sp": "Pa",
    "d2m": "K",
    "u10": "m s-1",
    "v10": "m s-1",
    "sh2": "kg kg-1",
    "wdir10": "degree",
    "tp": "kg m**-2",
}


# %%
def is_interactive():
    import __main__ as main
    return not hasattr(main, '__file__') or 'ipykernel' in sys.argv[0]


# %%
# ============================================================
# Processing logic
# ============================================================

def check_existing_data_in_zarr(zarr_store, day, var_name, freq="1h"):
    # open_zarr_safe() retries on NFS stale-file-handle errors (errno 116) -- this call runs
    # once per day (365x/job), and running multiple clusters concurrently against the same
    # store (dgx + its-head) makes a transient stale handle a real, observed failure mode
    # here (confirmed in production: tp/2023 crashed twice on its-head with exactly this
    # error, from this exact unprotected call -- every other zarr-open path in this codebase
    # already goes through this wrapper).
    ds = open_zarr_safe(zarr_store)
    if var_name not in ds.data_vars:
        return False
    day_dt = pd.to_datetime(day, format="%Y%m%d")
    if freq == "1h":
        day_times = pd.date_range(start=day_dt, end=day_dt + pd.Timedelta(hours=23), freq="1h")
    try:
        day_data = ds[var_name].sel(time=day_times)
    except KeyError:
        return False
    has_non_nan = day_data.notnull().any().compute()
    return bool(has_non_nan)


# %%
def normalize_time(ds):
    has_time = "time" in ds.coords
    has_valid = "valid_time" in ds.coords
    if has_time and has_valid:
        same = np.array_equal(ds["time"].values, ds["valid_time"].values)
        if not same:
            ds = ds.swap_dims({'time': 'valid_time'}).drop_vars('time').rename({'valid_time': 'time'})
            return ds
        return ds
    return ds


def daily_processing(var_name, date, time_chunk, x_chunk, y_chunk, x_start, y_start, nx, ny):
    x_end = x_start + nx
    y_end = y_start + ny

    if var_name != 'tp':
        files = glob.glob(f'{data_source_dir}/{date}/*2dvaranl*')

        def extract_hour(file):
            match = re.search(r't(\d{2})z', file)
            if match:
                return int(match.group(1))
            return 0

        sorted_files = sorted(files, key=extract_hour)
    else:
        files = glob.glob(f'{data_source_dir}/{date}/*pcp_01h*')

        def extract_hour(file):
            m = re.search(r'\.(\d{10})\.pcp_01h', file)
            if m:
                datetime_str = m.group(1)
                hour = int(datetime_str[-2:])
                return hour
            return 0

        sorted_files = sorted(files, key=extract_hour)

    def preprocess(ds):
        return ds.isel(y=slice(y_start, y_end), x=slice(x_start, x_end))

    ds = xr.open_mfdataset(
        sorted_files, concat_dim='time', combine='nested', parallel=True,
        preprocess=preprocess,
        engine="cfgrib",
        backend_kwargs={'indexpath': None, 'filter_by_keys': {'shortName': GRIB_SHORTNAME[var_name]}},
    )
    ds = normalize_time(ds)

    date_str = str(date)
    full_day_times = pd.date_range(
        start=pd.to_datetime(date_str, format="%Y%m%d"), periods=24, freq="h",
    )
    if ds.time.size < 24 or not np.array_equal(
        ds.time.values.astype("datetime64[ns]").astype("int64"),
        full_day_times.values[:ds.time.size].astype("datetime64[ns]").astype("int64"),
    ):
        ds = ds.reindex(time=full_day_times)

    ds = ds.chunk({'time': time_chunk, 'y': y_chunk, 'x': x_chunk})
    return ds


# ============================================================
# Daily Process + Write
# ============================================================

def process_and_write_single_day(
    date, var_name, zarr_store, dates, full_dates, time_chunk, x_chunk, y_chunk,
    x_start, y_start, nx, ny,
):
    if check_existing_data_in_zarr(zarr_store, date, var_name):
        print(f"[skip] {date} already exists in {zarr_store} for {var_name}")
        return

    try:
        ds = daily_processing(var_name, date, time_chunk, x_chunk, y_chunk, x_start, y_start, nx, ny)

        # Ensure source units are set (cfgrib may omit them).
        if not ds[var_name].attrs.get("units") and var_name in _GRIB_SOURCE_UNITS:
            ds[var_name].attrs["units"] = _GRIB_SOURCE_UNITS[var_name]
        ds = apply_var_attrs(ds, var_name)

        # write_region() (data_utils/zarr_io.py) handles region computation, dropping
        # non-dim coords, and -- critically -- stripping _FillValue/missing_value from
        # attrs/encoding before the region-mode to_zarr call. Without that stripping,
        # apply_var_attrs()'s explicit attrs["_FillValue"] collides with the encoding
        # xarray auto-derives from the already-initialized zarr array, and every single
        # write fails with "failed to prevent overwriting existing key _FillValue in
        # attrs" (confirmed in production: every write attempt across every day/var/year
        # failed this way, silently, since the exception is caught below and printed,
        # not raised -- jobs showed SLURM-level COMPLETED having written zero real data).
        write_region(
            ds[[var_name]], zarr_store, full_dates,
            {"time": time_chunk, "y": y_chunk, "x": x_chunk},
        )
        print(f"[write] {date}: wrote {var_name} to {zarr_store}")

    except Exception as e:
        print(f"[error] Failed on {date} for {var_name}: {e}")


# %%
if __name__ == "__main__":
    # %%
    parser = argparse.ArgumentParser(
        description="Process daily URMA GRIB2 files into yearly Zarr store."
    )
    parser.add_argument(
        "--var_name",
        type=str,
        default="si10" if is_interactive() else None,
        choices=ALL_VARS,
        help="Internal variable name (si10, u10, v10, t2m, d2m, sh2, sp, wdir10, tp)"
    )
    parser.add_argument(
        "--year",
        type=int,
        default=2025 if is_interactive() else None,
        help="Year to process (e.g., 2025)"
    )
    parser.add_argument("--full-start-year", type=int, default=2010)
    parser.add_argument("--full-end-year", type=int, default=2040)
    parser.add_argument(
        "--region", type=str, default=None,
        help="Region config name under configs/regions/ (e.g. New_York, New_Mexico) -- "
             "supplies the crop (grid.URMA) and output location (data_root/region_tag). "
             "REQUIRED, no default -- an implicit New_York default previously risked "
             "silently running against the wrong region's data if omitted."
    )
    parser.add_argument(
        "--init-only", action="store_true",
        help="Only ensure the output zarr store/variable skeleton exists, then exit -- no "
             "GRIB reads, no data written. Run this once per var, serially (one process at a "
             "time, not via sbatch), before fanning out the real per-var/year jobs in "
             "parallel -- otherwise multiple jobs can simultaneously see the store doesn't "
             "exist yet and race to create it (zarr.errors.GroupNotFoundError / 'Time "
             "coordinate mismatch' / stale-handle failures -- observed in production from "
             "this exact race for a brand-new region)."
    )

    if is_interactive():
        args, unknown = parser.parse_known_args()
    else:
        args = parser.parse_args()

    if args.region is None:
        parser.error("--region is required (e.g. --region New_Mexico) -- no default, to avoid silently running against the wrong region's data.")

    var_name = args.var_name
    YEAR = args.year

    # Region-specific crop and output location -- see configs/regions/{region}.yaml.
    region_grid_raw = load_region_grid(args.region, "URMA")
    region_vars = load_region_vars(args.region)
    # grid.URMA is {type, dims, inner, outer} when compute_and_write_region_crop.py was run with a
    # halo (URMA's own wide-context crop -- see update_region_config's crop_outer docstring),
    # or flat {type, dims, n0, n1, crop, bbox} otherwise (New York's entry, and any region
    # derived without a halo). Production processing uses the OUTER crop: met-data-products'
    # job is to fully contain the domain every downstream project will ever need from this
    # region in one store, so no project has to come back here again -- per-project narrowing
    # to the tight training footprint happens downstream, using the inner_mask/inner_* attrs
    # cropped_orography.nc carries (see write_cropped_orography), not here. dims lives at the
    # top level either way (shared between inner/outer -- same grid, same dim names).
    if "inner" in region_grid_raw:
        region_grid = {"dims": region_grid_raw["dims"], **region_grid_raw["outer"]}
    else:
        region_grid = region_grid_raw
    assert region_grid["dims"] == ["y", "x"]
    y_start = region_grid["crop"]["y_start"]
    x_start = region_grid["crop"]["x_start"]
    ny = region_grid["n0"]
    nx = region_grid["n1"]
    data_root = region_vars["data_root"]
    region_tag = region_vars["region_tag"]
    if not data_root:
        raise ValueError(f"configs/regions/{args.region}.yaml has no data_root set yet.")
    zarr_store = f"{data_root}/URMA_{region_tag}/URMA_{region_tag}.zarr"
    orog_path = Path(f"{data_root}/URMA_{region_tag}/cropped_orography.nc")

    # %%
    full_dates = pd.date_range(
        f"{args.full_start_year}-01-01T00", f"{args.full_end_year}-12-31T23", freq="h",
    )
    dates = pd.date_range(start=f'{YEAR}-01-01T00', end=f'{YEAR}-12-31T23', freq='h')
    yyyymmdd = pd.Series(dates.year * 10000 + dates.month * 100 + dates.day).unique()
    time_chunk = 6
    # Spatial chunk = the full cropped domain, taken from the orography-derived region_grid
    # (ny/nx above), not an arbitrary constant -- every real write already covers the full
    # cropped extent per day (see daily_processing's y_start:y_end/x_start:x_end), so a
    # single spatial chunk matches the actual write pattern exactly, with no partial-chunk
    # misalignment regardless of which region/footprint (inner vs outer) is in use.
    y_chunk = ny
    x_chunk = nx

    # %%
    cpus = get_slurm_cpus()
    print(cpus)

    # Initialize zarr for this variable using the region's cropped orography for spatial
    # structure -- read from cropped_orography.nc, written once ahead of time by
    # compute_and_write_region_crop.py --update-config, not re-derived in-memory here
    # (that duplicated the crop logic in two places; the crop tool is now the single
    # place that persists it). URMA_{region_tag}.zarr uses the wider OUTER footprint (when
    # this region has a halo split) so met-data-products fully contains the domain every
    # downstream project needs from this region in one store -- per-project narrowing to the
    # tight training window happens downstream, via inner_mask/inner_* attrs on
    # cropped_orography.nc, not here. A flat crop (New York, or any region derived without a
    # halo) has no inner/outer split -- the whole file already IS the region's one footprint.
    def _get_template():
        if not orog_path.exists():
            raise FileNotFoundError(
                f"{orog_path} not found -- run compute_and_write_region_crop.py --product URMA "
                f"--mode boundary --region-config configs/regions/{args.region}.yaml "
                f"--update-config first (it writes this file as a side effect)."
            )
        # Already at the OUTER extent when this region has a halo split (see
        # write_cropped_orography) -- used as-is, no slicing. init_zarr() only reads this
        # dataset's dims/coords for shape, so its inner_mask var / inner_* attrs are NOT
        # copied into the zarr store -- consult cropped_orography.nc directly for those.
        return xr.open_dataset(orog_path)

    chunks = {"time": time_chunk, "y": y_chunk, "x": x_chunk}
    zarr_sync = zarr.ProcessSynchronizer(f"{zarr_store}.sync")
    ensure_store(
        zarr_store, full_dates, var_name, _get_template, chunks,
        global_title=f"{region_tag} Remapped Meteorological Dataset",
        synchronizer=zarr_sync,
    )

    if args.init_only:
        print(f"[init-only] {zarr_store} ready for '{var_name}'")
        sys.exit(0)

    # 2. Process each day in parallel batches.
    batch_size = 30
    for i in tqdm(range(0, len(yyyymmdd), batch_size), desc=f"{var_name} {YEAR}"):
        batch_dates = yyyymmdd[i: i + batch_size]
        Parallel(n_jobs=cpus, backend="loky", verbose=0)(
            delayed(process_and_write_single_day)(
                date, var_name, zarr_store, dates, full_dates,
                time_chunk, x_chunk, y_chunk, x_start, y_start, nx, ny,
            )
            for date in batch_dates
        )

# %%
