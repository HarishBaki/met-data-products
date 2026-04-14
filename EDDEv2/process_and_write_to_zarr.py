# %%
"""
Process EDDE_V2 hourly files into a single Zarr store, one variable per month.
- Source root folder: /network/rit/lab/basulab/RAW_DATA/EDDE_V2/hourly/WRF-MPI
- Target root folder: /network/rit/lab/basulab/Projects/DFS/DATA/EDDEv2_NYS/hourly/WRF-MPI
- configurable runs and corresponding hourly time axis: 
    - Historical: 1985-2014
    - SSP2-4.5: 2025-2100
    - SSP3-7.0: 2025-2100
- variables: si10, t2m, sp, u10, v10, d2m, wdir10, tp
"""

import glob
import os
from pathlib import Path
from typing import Dict, List, Optional

import argparse
import time
import dask.array as da
import numpy as np
import pandas as pd
import xarray as xr
import zarr
from joblib import Parallel, delayed
from tqdm import tqdm

RAW_ROOT = Path("/network/rit/lab/basulab/RAW_DATA/EDDE_V2/hourly/WRF-MPI")
OUTPUT_ROOT = Path("/network/rit/lab/basulab/Projects/DFS/DATA/EDDEv2_NYS/hourly/WRF-MPI")

# File prefix -> source variable name in the NetCDF files.
FILE_TO_SOURCE = {
    "pr": "PRECIP",
    "ps": "PSFC",
    "td": "DEWPT",
    "ts": "T2",
    "wdirs": "WDIR10",
    "wspds": "WSPD10",
    "ua": "UE10",
    "va": "VE10",
}

# Source variable name -> file prefix.
SOURCE_TO_FILE = {v: k for k, v in FILE_TO_SOURCE.items()}

# Our intended variable names to be renamed (source var -> standardized var).
rename_map = {
    "PSFC": "sp",
    "T2": "t2m",
    "DEWPT": "d2m",
    "PRECIP": "tp",
    "WSPD10": "si10",
    "WDIR10": "wdir10",
    "UE10": "u10",
    "VE10": "v10",
}

# Standardized var name -> source variable name.
VAR_NAME_TO_SOURCE = {v: k for k, v in rename_map.items()}

full_years_range = {
    "Historical": (1985, 2014),
    "SSP2-4.5": (2025, 2100),
    "SSP3-7.0": (2025, 2100),
}

VAR_LONGNAME = {
    "si10": "10 m wind speed",
    "t2m": "2 m air temperature",
    "sp": "surface pressure",
    "d2m": "2 m dew point temperature",
    "u10": "10 m eastward wind",
    "v10": "10 m northward wind",
    "wdir10": "10 m wind direction",
    "tp": "total precipitation",
}
VAR_UNITS = {
    "si10": "m s-1",          # 10 m wind speed
    "t2m": "K",               # 2 m air temperature
    "sp": "hPa",               # surface pressure
    "d2m": "K",               # 2 m dew point temperature
    "u10": "m s-1",           # 10 m eastward wind
    "v10": "m s-1",           # 10 m northward wind
    "wdir10": "degree",       # 10 m wind direction
    "tp": "kg m-2",                # total precipitation (hourly accumulation)
}

UNIT_CONVERSIONS = {
    "tp": {
        "kg m-2": ("kg m-2", 1.0),
        "kg m**-2": ("kg m-2", 1.0),
        "mm": ("kg m-2", 1.0),
        "m": ("kg m-2", 1000.0),
    },
    "sp": {
        "hPa": ("hPa", 1.0),
        "Pa": ("hPa", 0.01),
    },
    "si10": {
        "m s-1": ("m s-1", 1.0),
        "m s**-1": ("m s-1", 1.0),
    },
    "u10": {
        "m s-1": ("m s-1", 1.0),
        "m s**-1": ("m s-1", 1.0),
    },
    "v10": {
        "m s-1": ("m s-1", 1.0),
        "m s**-1": ("m s-1", 1.0),
    },
}

UNIT_ALIASES = {
    "kg m-2": "kg m-2",
    "kg m^-2": "kg m-2",
    "kg m**-2": "kg m-2",
    "mm": "kg m-2",
    "m": "m",
    "pa": "Pa",
    "hpa": "hPa",
    "m s-1": "m s-1",
    "m s^-1": "m s-1",
    "m s**-1": "m s-1",
    "degree": "degree",
    "degrees": "degree",
}

DERIVED_VARS: Dict[str, List[str]] = {}

TIME_CHUNK = 24  # 6 * 1h = 6 hours
BATCH_SIZE = 64  # timestamps per parallel batch
LAT_MIN = 38
LAT_MAX = 48
LON_MIN = -82
LON_MAX = -68
y_min, y_max = 231, 343
x_min, x_max = 339, 453

def find_reference_file_for_var(var_name: str, run_type: str) -> Path:
  """Grab the first available nc for a specific variable."""
  if var_name in DERIVED_VARS:
    deps = DERIVED_VARS[var_name]
    var_name = deps[0]
  if var_name not in VAR_NAME_TO_SOURCE:
    raise KeyError(f"Unknown variable: {var_name}")
  source_var = VAR_NAME_TO_SOURCE[var_name]
  if source_var not in SOURCE_TO_FILE:
    raise KeyError(f"Missing file prefix for source var: {source_var}")
  file_prefix = SOURCE_TO_FILE[source_var]
  run_root = run_folder(run_type)
  candidates = sorted(
      glob.glob(str(run_root / "*" / f"{file_prefix}*.nc"))
  )
  if not candidates:
    raise FileNotFoundError(f"No nc files found for {var_name} under {RAW_ROOT}")
  return Path(candidates[0])

def crop_region(ds: xr.Dataset) -> xr.Dataset:
  """Crop to NYS bounding box through index slicing."""
  return ds.isel(y=slice(y_min, y_max), x=slice(x_min, x_max))


def run_folder(run_type: str) -> Path:
  years = full_years_range[run_type]
  return RAW_ROOT / run_type / f"{years[0]}-{years[1]}"


def month_from_filename(path: Path) -> pd.Timestamp:
  match = pd.to_datetime(path.name.split(".")[-3], format="%Y-%m")
  return match


def files_for_month(run_type: str, target_month: pd.Timestamp, var_name: str) -> List[str]:
  source_var = VAR_NAME_TO_SOURCE[var_name]
  file_prefix = SOURCE_TO_FILE[source_var]
  year_folder = run_folder(run_type) / f"{target_month.year}"
  pattern = f"{file_prefix}.*.{target_month.strftime('%Y-%m')}.raw.nc"
  return sorted(glob.glob(str(year_folder / pattern)))


def process_single_month(
    target_month: pd.Timestamp,
    renamed_var: str,
    run_type: str,
) -> Optional[xr.Dataset]:
  files = files_for_month(run_type, target_month, renamed_var)
  if not files:
    print(f"[skip] no files for {target_month.strftime('%Y-%m')} ({run_type})")
    return None

  ds = xr.open_mfdataset(
      files,
      engine="netcdf4",
      combine="by_coords",
      parallel=False,
      autoclose=True,
  )
  ds = ds.assign_coords(
      latitude=(("y", "x"), ds["lat"].values),
      longitude=(("y", "x"), ds["lon"].values),
  ).drop_vars(["lat", "lon"], errors="ignore")
  ds = ds.assign_coords(longitude=((ds.longitude + 360) % 360))
  ds = ds.drop_vars(["mtime", "y", "x"], errors="ignore")

  ds = ds.rename({k: v for k, v in rename_map.items() if k in ds.data_vars})
  ds = crop_region(ds)
  ds = ds.transpose("time", "y", "x")
  return ds


def init_zarr(
    full_times: pd.DatetimeIndex,
    lat: xr.DataArray,
    lon: xr.DataArray,
    var_name: str,
    var_attrs: Dict[str, object],
    output_zarr: str,
    zarr_sync: zarr.ProcessSynchronizer,
    mode: str = "w",
    write_global_attrs: bool = False,
):
  y_size, x_size = lat.shape
  chunks = (TIME_CHUNK, y_size, x_size)

  data = da.full((full_times.size, y_size, x_size), np.nan, chunks=chunks, dtype=np.float32)
  ds_init = xr.Dataset(
      {var_name: xr.DataArray(data, dims=("time", "y", "x"))},
      coords={"time": full_times, "latitude": lat, "longitude": lon},
  )
  if write_global_attrs:
    ds_init.attrs = {
        "title": "ERA5 hourly NYS subset",
        "Conventions": "CF-1.8",
        "history": "Initialized empty Zarr store",
    }
  attrs = dict(var_attrs or {})
  attrs.setdefault("long_name", VAR_LONGNAME.get(var_name))
  attrs.setdefault("units", VAR_UNITS.get(var_name))
  attrs.setdefault("_FillValue", np.nan)
  attrs.setdefault("missing_value", np.nan)
  ds_init[var_name].attrs = attrs
  ds_init.to_zarr(
      output_zarr,
      mode=mode,
      compute=False,
      zarr_format=2,
      synchronizer=zarr_sync,
  )


def open_zarr_safe(
    zarr_store: str,
    zarr_sync: zarr.ProcessSynchronizer,
    attempts: int = 5,
    base_delay: float = 1.0,
) -> xr.Dataset:
  for attempt in range(1, attempts + 1):
    try:
      return xr.open_zarr(zarr_store, consolidated=False, synchronizer=zarr_sync)
    except OSError as exc:
      if getattr(exc, "errno", None) != 116 or attempt == attempts:
        raise
      time.sleep(base_delay * attempt)


def apply_var_attrs_and_units(
    ds: xr.Dataset, var_name: str, existing_units: Optional[str]
) -> xr.Dataset:
  if var_name not in ds.data_vars:
    return ds
  attrs = dict(ds[var_name].attrs)
  if not attrs.get("long_name"):
    if var_name in VAR_LONGNAME:
      attrs["long_name"] = VAR_LONGNAME[var_name]
  src_units = attrs.get("units")
  target_units = existing_units or VAR_UNITS.get(var_name) or src_units
  src_norm = normalize_units(src_units)
  target_norm = normalize_units(target_units)
  if not src_units and target_units:
    attrs["units"] = target_units
    ds[var_name].attrs = attrs
    return ds
  if src_units and target_units and src_norm != target_norm:
    conv = (
        UNIT_CONVERSIONS.get(var_name, {}).get(src_units)
        or UNIT_CONVERSIONS.get(var_name, {}).get(src_norm)
    )
    if conv is None or normalize_units(conv[0]) != target_norm:
      raise ValueError(
          f"No unit conversion available for {var_name}: {src_units} -> {target_units}"
      )
    out_units, factor = conv
    if factor != 1.0:
      ds[var_name] = ds[var_name] * factor
    attrs["units"] = out_units
  ds[var_name].attrs = attrs
  return ds


def normalize_units(units: Optional[str]) -> Optional[str]:
  if not units:
    return units
  key = " ".join(units.strip().lower().split())
  return UNIT_ALIASES.get(key, units)


def ensure_store(
    full_times: pd.DatetimeIndex,
    var_name: str,
    run_type: str,
    output_zarr: str,
    zarr_sync: zarr.ProcessSynchronizer,
) -> xr.Dataset:
  if os.path.exists(output_zarr):
    ds = open_zarr_safe(output_zarr, zarr_sync)
    if not np.array_equal(pd.to_datetime(ds.time.values), pd.to_datetime(full_times.values)):
      raise ValueError("Time axis mismatch in existing Zarr store.")
    if var_name not in ds.data_vars:
      ref_file = find_reference_file_for_var(var_name, run_type)
      ref_month = month_from_filename(ref_file)
      template = process_single_month(ref_month, var_name, run_type)
      if template is None:
        raise RuntimeError("Failed to build template dataset for initialization.")
      template = apply_var_attrs_and_units(template, var_name, existing_units=None)
      init_zarr(
          full_times,
          ds.latitude,
          ds.longitude,
          var_name,
          dict(template[var_name].attrs),
          output_zarr,
          zarr_sync,
          mode="a",
          write_global_attrs=False,
      )
    return ds

  ref_file = find_reference_file_for_var(var_name, run_type)
  ref_month = month_from_filename(ref_file)
  template = process_single_month(ref_month, var_name, run_type)
  if template is None:
    raise RuntimeError("Failed to build template dataset for initialization.")
  template = apply_var_attrs_and_units(template, var_name, existing_units=None)
  init_zarr(
      full_times,
      template.latitude,
      template.longitude,
      var_name,
      dict(template[var_name].attrs),
      output_zarr,
      zarr_sync,
      mode="w",
      write_global_attrs=True,
  )
  return open_zarr_safe(output_zarr, zarr_sync)


def has_missing_data_for_times(
    zarr_store: str,
    times: pd.DatetimeIndex,
    var_name: str,
    zarr_sync: zarr.ProcessSynchronizer,
) -> bool:
  ds = open_zarr_safe(zarr_store, zarr_sync)
  if var_name not in ds.data_vars:
    return True
  data = ds[var_name].reindex(time=times)
  has_missing = data.isnull().any().compute()
  return bool(has_missing)


def write_one_time(
    target_month: pd.Timestamp,
    var_name: str,
    full_times: pd.DatetimeIndex,
    run_type: str,
    output_zarr: str,
    zarr_sync: zarr.ProcessSynchronizer,
):
  ds = process_single_month(target_month, var_name, run_type)
  if ds is None:
    return

  ds = ds.sortby("time")
  times = pd.DatetimeIndex(ds.time.values)
  if not has_missing_data_for_times(output_zarr, times, var_name, zarr_sync):
    print(f"[skip] {target_month.strftime('%Y%m')} already complete in {output_zarr} for {var_name}")
    return

  existing = open_zarr_safe(output_zarr, zarr_sync)
  existing_units = None
  if var_name in existing.data_vars:
    existing_units = existing[var_name].attrs.get("units")
  ds = apply_var_attrs_and_units(ds, var_name, existing_units=existing_units)
  ds = ds.chunk({"time": TIME_CHUNK, "y": ds.sizes["y"], "x": ds.sizes["x"]})
  start_idx = int(np.searchsorted(full_times.values, np.array(times[0], dtype="datetime64[ns]")))
  end_idx = int(np.searchsorted(full_times.values, np.array(times[-1], dtype="datetime64[ns]"))) + 1
  region = {"time": slice(start_idx, end_idx)}
  write_ds = xr.Dataset({var_name: ds[var_name]})
  write_ds = write_ds.drop_vars(["latitude", "longitude"], errors="ignore")
  write_ds = write_ds.assign_coords(time=ds.time)
  write_ds.to_zarr(
      output_zarr,
      mode="a",
      region=region,
      consolidated=False,
      zarr_format=2,
      align_chunks=True,
      safe_chunks=False,
      synchronizer=zarr_sync,
  )
  print(f"[write] {target_month.strftime('%Y%m')} -> {var_name} region {region}")

# %%
if __name__ == "__main__":
  # %%
  parser = argparse.ArgumentParser(description="Process EDDEv2 data to Zarr.")
  parser.add_argument("--run-type", type=str, default="SSP2-4.5", help="Run type: Historical, SSP2-4.5, SSP3-7.0")
  parser.add_argument(
      "--var-name",
      type=str,
      default="si10",
      choices=sorted(VAR_LONGNAME.keys()),
      help="Variable to process (one at a time).",
  )
  parser.add_argument("--process-start", default="2025-01", help="Start month (inclusive), e.g., 2017-12")
  parser.add_argument("--process-end", default="2030-12", help="End month (inclusive), e.g., 2025-12")
  args, _ = parser.parse_known_args()

  OUTPUT_ZARR = str(OUTPUT_ROOT / f"{args.run_type}.zarr")
  ZARR_SYNC_PATH = f"{OUTPUT_ZARR}.sync"
  ZARR_SYNC = zarr.ProcessSynchronizer(ZARR_SYNC_PATH)

  full_times = pd.date_range(
      f"{full_years_range[args.run_type][0]}-01-01T00",
      f"{full_years_range[args.run_type][1]}-12-31T23",
      freq="1h",
  )

  dates = pd.date_range(start=args.process_start, end=args.process_end, freq="MS")
  var_name = args.var_name

  # %%
  ensure_store(full_times, var_name, args.run_type, OUTPUT_ZARR, ZARR_SYNC)

  cpus = int(os.environ.get("SLURM_CPUS_ON_NODE", os.cpu_count() or 1))
  for i in tqdm(range(0, len(dates), BATCH_SIZE), desc="EDDEv2->Zarr"):
    chunk_times = dates[i : i + BATCH_SIZE]
    Parallel(n_jobs=cpus, backend="loky")(
        delayed(write_one_time)(ts, var_name, full_times, args.run_type, OUTPUT_ZARR, ZARR_SYNC)
        for ts in chunk_times
    )

# %%
