# %%
import numpy as np
import pandas as pd
import xarray as xr
import dask
import os, sys
import glob
import zarr
from joblib import Parallel, delayed
import os
import dask.array as da
import os, sys, time, glob, re
from tqdm import tqdm
import argparse

project_dir = '/network/rit/lab/basulab/Harish/DFS'

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
    "si10": "10si",       # wind speed at 10 m
    "i10fg": "i10fg",     # wind gust at 10 m
    "t2m": "2t",          # 2 m air temperature
    "sp": "sp",           # surface pressure
    "d2m": "2d",          # 2 m dew point
    "u10": "10u",         # eastward wind at 10 m
    "v10": "10v",         # northward wind at 10 m
    "sh2": "2sh",         # 2 m specific humidity
    "wdir10": "10wdir",   # 10 m wind direction
    "tp": "tp",           # total precipitation
}
VAR_LONGNAME = {
    "si10": "10 m wind speed",
    "i10fg": "10 m wind gust",
    "t2m": "2 m air temperature",
    "sp": "surface pressure",
    "d2m": "2 m dew point temperature",
    "u10": "10 m eastward wind",
    "v10": "10 m northward wind",
    "sh2": "2 m specific humidity",
    "wdir10": "10 m wind direction",
    "tp": "total precipitation",
}
VAR_UNITS = {
    "si10": "m s-1",          # 10 m wind speed
    "i10fg": "m s-1",         # 10 m wind gust
    "t2m": "K",               # 2 m air temperature
    "sp": "Pa",               # surface pressure
    "d2m": "K",               # 2 m dew point temperature
    "u10": "m s-1",           # 10 m eastward wind
    "v10": "m s-1",           # 10 m northward wind
    "sh2": "kg kg-1",         # 2 m specific humidity
    "wdir10": "degree",       # 10 m wind direction
    "tp": "m",                # total precipitation (hourly accumulation)
}

# %%
def is_interactive():
    import __main__ as main
    return not hasattr(main, '__file__') or 'ipykernel' in sys.argv[0]

# %%
# ============================================================
# Zarr initialization
# ============================================================

def init_zarr_store(zarr_store, dates, var_name, orog_path=orog_path,
                    mode="w", write_global_attrs=False, time_chunk=time_chunk,x_chunk=x_chunk,y_chunk=y_chunk):

    orog = xr.open_dataset(orog_path)
    orog.attrs = {}

    shape = (len(dates),) + orog.orog.shape
    data = da.full(shape, np.nan, chunks=(time_chunk, y_chunk, x_chunk), dtype="float32")

    base = xr.DataArray(
        data,
        dims=("time", "y", "x"),
        coords={"time": dates,
                "latitude": orog.latitude,
                "longitude": orog.longitude},
        name=var_name,
    )

    ds_init = base.to_dataset()

    # Only assign global attributes ONCE
    if write_global_attrs:
        ds_init.attrs = {
            "title": "NYS Remapped Meteorological Dataset",
            "Conventions": "CF-1.8",
            "history": "Initialized empty Zarr store",
        }

    ds_init[var_name].attrs = {
        "long_name": VAR_LONGNAME[var_name],
        "units": VAR_UNITS[var_name],
        "_FillValue": np.nan,
        "missing_value": np.nan,
    }

    ds_init.to_zarr(zarr_store, mode=mode, compute=False, zarr_format=2)

def ensure_initialized(zarr_store, full_dates, var_name,
                       orog_path=orog_path,
                       time_chunk=time_chunk, x_chunk=x_chunk, y_chunk=y_chunk):
    """
    Ensure Zarr store is initialized for the given variable.
    - Creates the store if missing (with global attrs)
    - Adds variable if missing (without touching global attrs)
    - Enforces a consistent time coordinate for all variables
    """

    # ---------------------------------------------------------
    # CASE 1 — STORE DOESN'T EXIST → CREATE and write global attrs
    # ---------------------------------------------------------
    if not os.path.exists(zarr_store):
        print(f"[init] Creating new store {zarr_store} with variable '{var_name}'")
        init_zarr_store(
            zarr_store,
            full_dates,
            var_name,
            orog_path=orog_path,
            mode="w",
            write_global_attrs=True,     # <-- IMPORTANT
            time_chunk=time_chunk,
            x_chunk=x_chunk,
            y_chunk=y_chunk,
        )
        return

    # ---------------------------------------------------------
    # CASE 2 — STORE EXISTS → Load metadata
    # ---------------------------------------------------------
    ds_meta = xr.open_zarr(zarr_store, consolidated=False)

    # Basic safety check
    if "time" not in ds_meta.coords:
        raise ValueError("Zarr store missing 'time' coordinate — cannot proceed.")

    # ---------------------------------------------------------
    # CHECK TIME CONSISTENCY
    # ---------------------------------------------------------
    same_len = ds_meta.sizes.get("time", -1) == full_dates.size
    same_vals = np.array_equal(
        pd.to_datetime(ds_meta.time.values),
        pd.to_datetime(full_dates.values),
    )

    if not (same_len and same_vals):
        raise ValueError("Time coordinate mismatch. Rebuild Zarr store.")

    # ---------------------------------------------------------
    # CASE 3 — VARIABLE MISSING → ADD IT
    # ---------------------------------------------------------
    if var_name not in ds_meta.data_vars:
        print(f"[init] Adding variable '{var_name}' to {zarr_store}")
        init_zarr_store(
            zarr_store,
            full_dates,
            var_name,
            orog_path=orog_path,
            mode="a",
            write_global_attrs=False,   # <-- VERY IMPORTANT
            time_chunk=time_chunk,
            x_chunk=x_chunk,
            y_chunk=y_chunk,
        )

    else:
        print(f"[init] Variable '{var_name}' already exists in {zarr_store} and is consistent.")

# ============================================================
# Processing logic
# ============================================================

def check_existing_data_in_zarr(zarr_store, day, var_name, freq="1h"):
    """
    Check if data for the given day and variable already exists in the Zarr store.
    Works for hourly URMA data.
    """

    ds = xr.open_zarr(zarr_store, consolidated=False)

    # Variable missing from store → no data
    if var_name not in ds.data_vars:
        return False

    # Build expected timestamps for the day
    day_dt = pd.to_datetime(day, format="%Y%m%d")

    if freq == "1h":
        # URMA hourly
        day_times = pd.date_range(start=day_dt, end=day_dt + pd.Timedelta(hours=23), freq="1h")

    # Select the day's block from the Zarr store
    try:
        day_data = ds[var_name].sel(time=day_times)
    except KeyError:
        return False  # Missing timestamps → no data written yet

    # If all NaNs → data does not exist
    has_non_nan = day_data.notnull().any().compute()

    return bool(has_non_nan)

# %%
def normalize_time(ds):
    has_time = "time" in ds.coords
    has_valid = "valid_time" in ds.coords

    # ----------------------------------------
    # CASE 1: Both exist
    # ----------------------------------------
    if has_time and has_valid:
        same = np.array_equal(ds["time"].values,ds["valid_time"].values)

        # --- CASE 1A: Not same → use valid_time
        if not same:
            # Use valid_time as the time dimension and remove time
            ds = ds.swap_dims({'time':'valid_time'}).drop_vars('time').rename({'valid_time':'time'})

            return ds

        # --- CASE 1B: both same → do nothing
        return ds

    # ----------------------------------------
    # CASE 2: One or none exists → do nothing
    # ----------------------------------------
    return ds

def daily_processing(var_name,date,time_chunk=time_chunk,x_chunk=x_chunk,y_chunk=y_chunk):
    # define the region of interest
    nx, ny = 288, 256
    x_start, x_end = 1800, 1800 + nx
    y_start, y_end = 830, 830 + ny

    # read files in sorted order.
    #  with keywords in the ascending order t00z, t01z, ... , t23z
    if var_name != 'tp':
        files = glob.glob(f'{data_source_dir}/{date}/*2dvaranl*')
        def extract_hour(file):
            # Match the pattern 'tXXz' where XX is the hour (e.g., t00z, t01z, etc.)
            match = re.search(r't(\d{2})z', file)
            if match:
                return int(match.group(1))  # Return the hour as an integer
            return 0  # Default in case no match is found (although unlikely here)

        # Sort the files by the extracted hour
        sorted_files = sorted(files, key=extract_hour)
    else:
        files = glob.glob(f'{data_source_dir}/{date}/*pcp_01h*')
        def extract_hour(file):
            # Extract the 10-digit datetime: YYYYMMDDHH
            # Example: "urma2p5.2019010105.pcp_01h" → "2019010105"
            m = re.search(r'\.(\d{10})\.pcp_01h', file)
            if m:
                datetime_str = m.group(1)
                hour = int(datetime_str[-2:])  # last two digits = hour
                return hour
            return 0
        sorted_files = sorted(files, key=extract_hour)

    def preprocess(ds):
        return ds.isel(y=slice(y_start,y_end),x=slice(x_start,x_end))

    ds = xr.open_mfdataset(sorted_files,concat_dim='time',combine='nested', parallel=True, preprocess=preprocess,
                        engine="cfgrib", backend_kwargs={'indexpath': None,'filter_by_keys': {'shortName': GRIB_SHORTNAME[var_name]}})
    
    # We are interested in valid_time dimension for time.
    # This step is very important, since for precipitation 'tp', valid_time is the end of accumulation time, and is of our interest.
    ds = normalize_time(ds)

    # Convert date to string (yyyymmdd)
    date_str = str(date)
    # Get all 24 hours for that day
    full_times = pd.date_range(start=pd.to_datetime(date_str, format="%Y%m%d"), periods=24, freq="h")

    # Sometimes time values are missing or unordered
    if ds.time.size < 24 or not np.array_equal(
            ds.time.values.astype("datetime64[ns]").astype("int64"),
            full_times.values[:ds.time.size].astype("datetime64[ns]").astype("int64")
        ):
        ds = ds.reindex(time=full_times)        # pad missing hours with NaNs

    ds = ds.chunk({'time': time_chunk, 'y': y_chunk, 'x': x_chunk})
    return ds
# %%
# ============================================================
# Zarr Write Helper
# ============================================================

def write_chunk(ds_chunk, zarr_store, region):
    """
    Write one day's chunk to the Zarr store using region writes.
    """
    ds_chunk.to_zarr(
        zarr_store,
        region=region,
        mode="a",
        zarr_format=2
    )


# ============================================================
# Daily Process + Write
# ============================================================

def process_and_write_single_day(date, var_name, zarr_store):
    """
    Process one day of URMA GRIB2 data and write directly to Zarr.
    """
    # --- Skip if already written ---
    if check_existing_data_in_zarr(zarr_store, date, var_name):
        print(f"[skip] {date} already exists in {zarr_store} for {var_name}")
        return

    try:
        # --- Read and crop GRIB for that day ---
        ds = daily_processing(var_name, date)

        # --- Compute indices into the yearly calendar ---
        time_indices = np.searchsorted(dates.values, ds.time.values)

        region = {"time": slice(time_indices[0], time_indices[-1] + 1)}

        # --- Drop unwanted metadata variables that cfgrib produces ---
        drop_list = [
            "time", "valid_time", "surface", "heightAboveGround",
            "step", "latitude", "longitude"
        ]
        drop_existing = [v for v in drop_list if v in ds.variables]
        ds_chunk = ds.drop_vars(drop_existing)

        # --- Write to Zarr ---
        write_chunk(ds_chunk, zarr_store, region)

        print(f"[write] {date}: wrote {var_name} → Zarr region {region}")

    except Exception as e:
        print(f"[error] Failed on {date} for {var_name}: {e}")
# %%
# ============================================================
# Parallel Loop Over All Days in the Year
# ============================================================

def get_slurm_cpus():
    return int(os.environ.get("SLURM_CPUS_ON_NODE", 1))

if __name__ == "__main__":
    # %%
    # -------------------------------
    # Argument parser
    # -------------------------------
    parser = argparse.ArgumentParser(
        description="Process daily URMA GRIB2 files into yearly Zarr store."
    )

    parser.add_argument(
        "--var_name",
        type=str,
        default="si10" if is_interactive() else None,
        choices=list(GRIB_SHORTNAME.keys()),
        help="Internal variable name (si10, u10, v10, t2m, d2m, sh2, sp, wdir10, tp)"
    )

    parser.add_argument(
        "--year",
        type=int,
        default=2020 if is_interactive() else None,
        help="Year to process (e.g., 2020)"
    )

    # Handle Jupyter/IPython
    if is_interactive():
        args, unknown = parser.parse_known_args()
    else:
        args = parser.parse_args()
    var_name = args.var_name
    YEAR = args.year

    # %%
    dates = pd.date_range(start=f'{YEAR}-01-01T00', end=f'{YEAR}-12-31T23', freq='h')
    yyyymmdd = pd.Series(dates.year*10000 + dates.month*100 + dates.day).unique()
    zarr_store = f'/network/rit/home/hb533188/basulab/Projects/DFS/DATA/URMA_NYS/{YEAR}.zarr'
    data_source_dir = '/network/rit/home/hb533188/basulab/RAW_DATA/URMA'
    time_chunk=24
    x_chunk=144
    y_chunk=128
    orog_path = f"{project_dir}/urma_nys_orography.nc"

    cpus = get_slurm_cpus()
    print(cpus)

    # 1. Initialize Zarr for this variable-year
    ensure_initialized(zarr_store, dates, var_name)

    # 2. Process each day in parallel batches
    batch_size = 30
    for i in tqdm(range(0, len(yyyymmdd), batch_size), desc=f"{var_name} {YEAR}"):
        batch_dates = yyyymmdd[i:i + batch_size]

        Parallel(n_jobs=cpus, backend="loky", verbose=0)(
            delayed(process_and_write_single_day)(date, var_name, zarr_store)
            for date in batch_dates
        )
# %%
# ============================================================
# Identify Missing Hours in Output
# ============================================================

# ds = xr.open_zarr(zarr_store)
# nan_mask = ds[var_name].isnull().any(dim=("y", "x"))

# nan_times = ds["time"].where(nan_mask).dropna("time").compute()

# nan_times.to_netcdf(f"nan_times_{var_name}_{YEAR}.nc")
# print(f"[done] Missing timestamps saved → nan_times_{var_name}_{YEAR}.nc")

# %%
