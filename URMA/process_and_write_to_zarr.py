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

from repo_utils import find_repo_root
from data_utils.zarr_io import (
    apply_var_attrs,
    ensure_store,
    get_slurm_cpus,
    has_missing_data,
    open_zarr_safe,
    write_region,
)

PROJECT_DIR = find_repo_root(__file__)
CFG_PATH = PROJECT_DIR / "data_utils" / "baseline_regrid.yaml"
with open(CFG_PATH, "r") as f:
    CFG = yaml.safe_load(f)

zarr_store = f'/network/rit/lab/basulab/Projects/DFS/DATA/URMA_NYS/URMA_NYS.zarr'
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
    ds = xr.open_zarr(zarr_store, consolidated=False)
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


def daily_processing(var_name, date, time_chunk, x_chunk, y_chunk):
    nx, ny = 288, 256
    x_start, x_end = 1800, 1800 + nx
    y_start, y_end = 830, 830 + ny

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
):
    if check_existing_data_in_zarr(zarr_store, date, var_name):
        print(f"[skip] {date} already exists in {zarr_store} for {var_name}")
        return

    try:
        ds = daily_processing(var_name, date, time_chunk, x_chunk, y_chunk)

        # Ensure source units are set (cfgrib may omit them).
        if not ds[var_name].attrs.get("units") and var_name in _GRIB_SOURCE_UNITS:
            ds[var_name].attrs["units"] = _GRIB_SOURCE_UNITS[var_name]
        ds = apply_var_attrs(ds, var_name)

        time_indices = np.searchsorted(full_dates.values, ds.time.values)
        region = {"time": slice(time_indices[0], time_indices[-1] + 1)}

        drop_list = [
            "time", "valid_time", "surface", "heightAboveGround",
            "step", "latitude", "longitude"
        ]
        drop_existing = [v for v in drop_list if v in ds.variables]
        ds_chunk = ds.drop_vars(drop_existing)
        ds_chunk.to_zarr(zarr_store, region=region, mode="a", zarr_format=2)
        print(f"[write] {date}: wrote {var_name} → Zarr region {region}")

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

    if is_interactive():
        args, unknown = parser.parse_known_args()
    else:
        args = parser.parse_args()

    var_name = args.var_name
    YEAR = args.year

    # %%
    full_dates = pd.date_range(
        f"{args.full_start_year}-01-01T00", f"{args.full_end_year}-12-31T23", freq="h",
    )
    dates = pd.date_range(start=f'{YEAR}-01-01T00', end=f'{YEAR}-12-31T23', freq='h')
    yyyymmdd = pd.Series(dates.year * 10000 + dates.month * 100 + dates.day).unique()
    time_chunk = 6
    y_chunk = 256
    x_chunk = 288
    orog_path = CFG["paths"]["urma_orog"]

    # %%
    cpus = get_slurm_cpus()
    print(cpus)

    # Initialize zarr for this variable using orography file for spatial structure.
    # zarr_io.init_zarr reads spatial dims/coords directly from the dataset.
    def _get_template():
        return xr.open_dataset(orog_path)

    chunks = {"time": time_chunk, "y": y_chunk, "x": x_chunk}
    ensure_store(
        zarr_store, full_dates, var_name, _get_template, chunks,
        global_title="NYS Remapped Meteorological Dataset",
    )

    # 2. Process each day in parallel batches.
    batch_size = 30
    for i in tqdm(range(0, len(yyyymmdd), batch_size), desc=f"{var_name} {YEAR}"):
        batch_dates = yyyymmdd[i: i + batch_size]
        Parallel(n_jobs=cpus, backend="loky", verbose=0)(
            delayed(process_and_write_single_day)(
                date, var_name, zarr_store, dates, full_dates,
                time_chunk, x_chunk, y_chunk,
            )
            for date in batch_dates
        )

# %%
