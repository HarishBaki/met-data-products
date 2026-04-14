# %%
import numpy as np
import pandas as pd
import xarray as xr
import os, sys, glob, re, time
import argparse
import dask.array as da
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2]
SRC_DIR = PROJECT_ROOT / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from project_paths import DATA_DIR, STATIC_DATA_DIR

# ============================================================
# Helpers
# ============================================================

def is_interactive():
    """Detect if running inside Jupyter/IPython."""
    import __main__ as main
    return not hasattr(main, "__file__") or "ipykernel" in sys.argv[0]

# Mapping for different MRMS products
DATA_CONFIG = {
    "MergedReflectivityQCComposite_00.50": {
        "glob": str(DATA_DIR / "MRMS_grib_data" / "CONUS" / "MergedReflectivityQCComposite_00.50" / "{day}" / "cropped_NYS" / "*.nc"),
        "var": "Reflectivity_composite",
    },
    "MergedReflectivityAtLowestAltitude_00.50": {
        "glob": str(DATA_DIR / "MRMS_grib_data" / "CONUS" / "MergedReflectivityAtLowestAltitude_00.50" / "{day}" / "cropped_NYS" / "*.nc"),
        "var": "Reflectivity_lowest",
    },
}

# Full multi-year calendar (5-min frequency)
DATES = pd.date_range("2018-01-01T00:00", "2027-12-31T23:59", freq="5min")

# ============================================================
# Zarr initialization
# ============================================================

def init_zarr_store(zarr_store, dates, var_name, mode="w", time_chunk=24):
    """Pre-allocate the full multi-year store for a single variable (lazy NaNs)."""
    orog = xr.open_dataset(STATIC_DATA_DIR / "orography.nc")
    orog.attrs = {}  # strip noisy attrs

    # Shape: (time, y, x)
    shape = (len(dates),) + orog.orog.shape

    # Lazy Dask array filled with NaNs (no data written yet)
    data = da.full(shape, np.nan, chunks=(time_chunk, -1, -1), dtype="float32")

    base = xr.DataArray(
        data,
        dims=("time", "y", "x"),
        coords={
            "time": dates,
            "latitude": orog.latitude,
            "longitude": orog.longitude,
        },
        name=var_name,
        attrs={
            "long_name": var_name.replace("_", " "),
            "units": "dBZ",
            "_FillValue": np.nan,
            "missing_value": np.nan,
        },
    )

    ds_init = base.to_dataset()

    # Global attrs
    ds_init.attrs = {
        "title": "MRMS Reflectivity Dataset",
        "source": "NOAA MRMS, remapped to orography grid",
        "Conventions": "CF-1.8",
        "history": "Initialized empty Zarr store for multi-year aggregation",
        "note": f"Variable {var_name} written on a 5-min grid (2018–2027)",
    }

    # Write only metadata (fast), enforce Zarr v2
    ds_init.to_zarr(zarr_store, mode=mode, zarr_format=2, compute=False)

def ensure_initialized(zarr_store, full_dates, var_name, time_chunk=24):
    """Initialize once per variable against the FULL multi-year calendar."""
    if not os.path.exists(zarr_store):
        print(f"[init] Creating {zarr_store} with {var_name}")
        init_zarr_store(zarr_store, full_dates, var_name, mode="w", time_chunk=time_chunk)
        return

    ds_meta = xr.open_zarr(zarr_store, consolidated=False)

    if "time" not in ds_meta.coords:
        raise ValueError("Existing store has no 'time' coordinate; cannot region-write.")

    same_len = ds_meta.sizes.get("time", -1) == full_dates.size
    same_vals = same_len and np.array_equal(
        pd.to_datetime(ds_meta.time.values), pd.to_datetime(full_dates.values)
    )
    if not same_vals:
        raise ValueError("Time coordinate mismatch — rebuild store before use.")

    if var_name not in ds_meta.data_vars:
        print(f"[init] Adding variable '{var_name}' to {zarr_store}")
        init_zarr_store(zarr_store, full_dates, var_name, mode="a", time_chunk=time_chunk)
    else:
        print(f"[init] {zarr_store} already has '{var_name}' with correct calendar.")

# ============================================================
# Processing logic
# ============================================================

def check_existing_data_in_zarr(zarr_store, day, data_type):
    """Check if data for the given day and variable already exists in the Zarr store."""

    cfg = DATA_CONFIG[data_type]
    var_name = cfg["var"]
    ds = xr.open_zarr(zarr_store, consolidated=False)

    if var_name not in ds.data_vars:
        return False  # Variable doesn't exist in store

    day_dt = pd.to_datetime(day, format="%Y%m%d")
    day_times = pd.date_range(start=day_dt, end=day_dt + pd.Timedelta(hours=23, minutes=55), freq="5min")

    # Check if all timestamps for the day are present and non-NaN
    day_data = ds[var_name].sel(time=day_times)
    if day_data.isnull().all():
        return False  # All data is NaN, so treat as non-existent

    return True  # Data exists for the day



def daily_processing(day:str, data_type:str):
    if data_type not in DATA_CONFIG:
        raise ValueError(f"Unknown data_type {data_type}")

    cfg = DATA_CONFIG[data_type]
    glob_pattern = cfg["glob"]
    var_name = cfg["var"]

    files = sorted(glob.glob(glob_pattern.format(day=day)))
    if not files:
        print(f"[skip] No files found for {day}")
        return

    # Parse times
    times = [pd.to_datetime(f.split("_")[-3], format="%Y%m%d-%H%M%S") for f in files]

    # Open dataset
    ds = xr.open_mfdataset(files, combine="by_coords", parallel=False, chunks={"time": 24})
    ds = ds.assign_coords(time=("time", times))
    ds = ds.isel(alt=0, drop=True)

    # Resample to 5-min grid, nearest within 2min tolerance
    ds = ds.resample(time="5min").nearest(tolerance="2min")

    # Normalize variable name
    raw_var = list(ds.data_vars)[0]
    if raw_var != var_name:
        ds = ds[[raw_var]].rename({raw_var: var_name})

    # Expected timeline (288 timesteps, 5-min interval)
    day_dt = pd.to_datetime(day, format="%Y%m%d")
    full_times = pd.date_range(
        start=day_dt,
        end=day_dt + pd.Timedelta(hours=23, minutes=55),
        freq="5min"
    )

    # Reindex to enforce full 288 timesteps per day
    # Check if missing or mismatched
    if ds.time.size != 288 or not np.array_equal(ds.time.values, full_times.values):
        print(f"[warn] {day}: expected 288 timesteps, got {ds.time.size}. Reindexing...")
        ds = ds.reindex(time=full_times)
    
    return ds

def write_chunk(ds_chunk, zarr_store, region):
    """
    Function to write a single chunk to the Zarr store.
    """
    ds_chunk.to_zarr(zarr_store, region=region, mode='a', zarr_format=2) # very important, since zarr written in v3 cannot be read by v2

def process_and_write_single_day(day: str, data_type: str, zarr_store: str):
    cfg = DATA_CONFIG[data_type]
    var_name = cfg["var"]

    # First ensure store is initialized
    ensure_initialized(zarr_store, DATES, var_name, time_chunk=24)

    # Check if data already exists for the day
    if check_existing_data_in_zarr(zarr_store, day, data_type):
        print(f"[skip] Data for {day} and variable '{var_name}' already exists in {zarr_store}.")
        return

    ds = daily_processing(day, data_type)

    # Index into the global calendar
    idx = pd.Index(DATES).get_indexer(ds.time.values)
    if (idx < 0).any():
        raise ValueError("Some timestamps not found in global calendar.")

    # Drop unused vars but keep time
    drop_these = ['time','lon','lat']
    ds_chunk = ds.drop_vars([v for v in drop_these if v in ds.variables])

    # Chunk along time dimension (24 timesteps = 2 hours)
    ds_chunk = ds_chunk.chunk({"time": 24})

    # Write to Zarr
    region = {"time": slice(idx[0], idx[-1] + 1)}
    write_chunk(ds_chunk, zarr_store, region)
    
    print(f"[write] {day} → {zarr_store}:{var_name} at {len(idx)} slots")

# ============================================================
# Main entry point
# ============================================================

# %%
if __name__ == "__main__":
    # %%
    parser = argparse.ArgumentParser("Process MRMS daily NetCDF into Zarr.")
    parser.add_argument("--data_type", "-t", default="MergedReflectivityQCComposite_00.50",
                        choices=DATA_CONFIG.keys(), help="MRMS variable type")
    parser.add_argument("--day", "-d", default="20201015",
                        help="Day string, e.g. 20201015")
    parser.add_argument("--zarr_store", "-z", default=str(DATA_DIR / "MRMS.zarr"),
                        help="Path to Zarr store")

    # If interactive (e.g., running with #%%, no CLI args) → just defaults
    if len(sys.argv) == 1:
        args = parser.parse_args([])   # no args, only defaults
    else:
        args, _ = parser.parse_known_args()
    data_type = args.data_type
    day = args.day
    zarr_store = str(Path(args.zarr_store).expanduser())
    if not Path(zarr_store).is_absolute():
        zarr_store = str(PROJECT_ROOT / zarr_store)
    # %%
    process_and_write_single_day(day, data_type, zarr_store)
# %%
