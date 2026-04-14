# %%
"""
Process GFS 0.25° analysis files (00/06/12/18Z) into a single Zarr store.
- Reads one time per worker, merges groups, splits pressure levels into per-level vars.
- Writes to /network/rit/lab/basulab/Projects/NASA/DATA/GFSp25_analysis_NYS.zarr
- Time axis: 2015-01-01 00Z through 2030-12-31 18Z, every 6 hours.
"""

import glob
import os
from pathlib import Path
from typing import Dict, List, Optional

import argparse
import dask.array as da
import numpy as np
import pandas as pd
import xarray as xr
from joblib import Parallel, delayed
from tqdm import tqdm

RAW_ROOT = Path("/network/rit/lab/basulab/RAW_DATA/GFS0p25")
OUTPUT_ZARR = "/network/rit/lab/basulab/Projects/NASA/DATA/GFSp25_analysis_NYS.zarr"

LEVELS = [500, 700, 850, 925, 1000]
SURFACE_VARS = ["sp", "gust", "prate", "lftx", "cape", "cin"]
HEIGHT10_VARS = ["u10", "v10"]
HEIGHT2_VARS = ["t2m", "d2m", "r2", "sh2"]
PRESSURE_SHORTNAMES = ["u", "v", "t", "r", "w", "gh"]

TIME_CHUNK = 16  # 16 * 6h ≈ 4 days
BATCH_SIZE = 64  # timestamps per parallel batch
LAT_MIN = 38
LAT_MAX = 48
LON_MIN = -82
LON_MAX = -68


def find_reference_file() -> Path:
  """Grab the first available GRIB for grid/coords."""
  candidates = sorted(glob.glob(str(RAW_ROOT / "*" / "*" / "gfs.t??z.pgrb2.0p25.f000")))
  if not candidates:
    raise FileNotFoundError(f"No GRIB files found under {RAW_ROOT}")
  return Path(candidates[0])


def normalize_time(ds: xr.Dataset, target_time: np.datetime64) -> xr.Dataset:
  # Drop any existing time/valid_time and cfgrib extras to avoid conflicts,
  # then add a fresh time dimension with the target timestamp.
  for coord in ["time", "valid_time", "step", "isobaricInhPa", "heightAboveGround", "surface"]:
    if coord in ds.coords or coord in ds.variables:
      ds = ds.drop_vars(coord, errors="ignore")

  ds = ds.expand_dims(time=[np.datetime64(target_time)])
  return ds


def crop_region(ds: xr.Dataset) -> xr.Dataset:
  """Crop to NYS bounding box (lat 38-48, lon -82 to -68). Handles 0-360 lon."""
  # Longitudes: convert requested bounds to dataset convention
  if float(ds.longitude.max()) > 180:  # 0-360
    lon_min = LON_MIN + 360 if LON_MIN < 0 else LON_MIN
    lon_max = LON_MAX + 360 if LON_MAX < 0 else LON_MAX
  else:
    lon_min, lon_max = LON_MIN, LON_MAX

  lat_slice = slice(LAT_MAX, LAT_MIN) if ds.latitude[0] > ds.latitude[-1] else slice(LAT_MIN, LAT_MAX)
  lon_slice = slice(lon_min, lon_max) if ds.longitude[0] < ds.longitude[-1] else slice(lon_max, lon_min)

  return ds.sel(latitude=lat_slice, longitude=lon_slice)


def open_group(file_path: Path, filter_by: Dict) -> xr.Dataset:
  return xr.open_dataset(
      file_path,
      engine="cfgrib",
      backend_kwargs={"filter_by_keys": filter_by, "indexpath": ""},
      chunks={"latitude": -1, "longitude": -1},
  )


def load_pressure_dataset(file_path: Path) -> xr.Dataset:
  datasets = []
  for short in PRESSURE_SHORTNAMES:
    ds = open_group(file_path, {"typeOfLevel": "isobaricInhPa", "shortName": short})
    datasets.append(ds.sel(isobaricInhPa=LEVELS))
  return xr.merge(datasets, compat="override")


def split_pressure_levels(ds_pl: xr.Dataset) -> xr.Dataset:
  out = {}
  for var_name, da_var in ds_pl.data_vars.items():
    for lvl in LEVELS:
      da_lvl = da_var.sel(isobaricInhPa=lvl).squeeze(drop=True)
      if "isobaricInhPa" in da_lvl.coords:
        da_lvl = da_lvl.drop_vars("isobaricInhPa")
      out[f"{var_name}_{int(lvl)}hpa"] = da_lvl
  return xr.Dataset(out)


def process_single_time(target_time: pd.Timestamp) -> Optional[xr.Dataset]:
  hour = target_time.strftime("%H")
  date_str = target_time.strftime("%Y%m%d")
  file_path = RAW_ROOT / date_str / hour / f"gfs.t{hour}z.pgrb2.0p25.f000"

  if not file_path.exists():
    print(f"[skip] Missing file for {target_time}: {file_path}")
    return None

  ds_surface = open_group(file_path, {"typeOfLevel": "surface"})
  ds_surface = xr.Dataset({v: ds_surface[v] for v in SURFACE_VARS if v in ds_surface})

  ds_m10 = open_group(file_path, {"typeOfLevel": "heightAboveGround", "level": 10})
  ds_m10 = xr.Dataset({v: ds_m10[v] for v in HEIGHT10_VARS if v in ds_m10})

  ds_m2 = open_group(file_path, {"typeOfLevel": "heightAboveGround", "level": 2})
  ds_m2 = xr.Dataset({v: ds_m2[v] for v in HEIGHT2_VARS if v in ds_m2})

  ds_pl = split_pressure_levels(load_pressure_dataset(file_path))

  ds = xr.merge([ds_surface, ds_m10, ds_m2, ds_pl], compat="override")
  ds = crop_region(ds)
  ds = normalize_time(ds, np.datetime64(target_time))
  ds = ds.transpose("time", "latitude", "longitude", ...)
  return ds


def init_zarr(full_times: pd.DatetimeIndex, template: xr.Dataset):
  cropped = crop_region(template)
  lat = cropped.latitude
  lon = cropped.longitude
  lat_size = lat.size
  lon_size = lon.size
  chunks = (TIME_CHUNK, lat_size, lon_size)

  data_vars = {}
  for name in template.data_vars:
    data = da.full((full_times.size, lat_size, lon_size), np.nan, chunks=chunks, dtype=np.float32)
    data_vars[name] = xr.DataArray(data, dims=("time", "latitude", "longitude"))

  ds_init = xr.Dataset(data_vars, coords={"time": full_times, "latitude": lat, "longitude": lon})
  ds_init.to_zarr(OUTPUT_ZARR, mode="w", compute=False, zarr_format=2)


def ensure_store(full_times: pd.DatetimeIndex) -> xr.Dataset:
  if os.path.exists(OUTPUT_ZARR):
    ds = xr.open_zarr(OUTPUT_ZARR, consolidated=False)
    if not np.array_equal(pd.to_datetime(ds.time.values), pd.to_datetime(full_times.values)):
      raise ValueError("Time axis mismatch in existing Zarr store.")
    return ds

  ref_file = find_reference_file()
  ref_date = ref_file.parent.parent.name
  ref_hour = ref_file.parent.name
  ref_time = pd.to_datetime(f"{ref_date}{ref_hour}", format="%Y%m%d%H")
  template = process_single_time(ref_time)
  if template is None:
    raise RuntimeError("Failed to build template dataset for initialization.")
  init_zarr(full_times, template)
  return xr.open_zarr(OUTPUT_ZARR, consolidated=False)


def write_one_time(ts: pd.Timestamp, full_times: pd.DatetimeIndex):
  ds = process_single_time(ts)
  if ds is None:
    return
  ds = ds.chunk({"time": 1, "latitude": ds.sizes["latitude"], "longitude": ds.sizes["longitude"]})
  idx = int(np.searchsorted(full_times.values, np.array(ts, dtype="datetime64[ns]")))
  region = {"time": slice(idx, idx + 1)}
  write_ds = xr.Dataset({k: ds[k] for k in ds.data_vars})
  write_ds = write_ds.drop_vars(["latitude", "longitude"], errors="ignore")
  write_ds = write_ds.assign_coords(time=ds.time)
  write_ds.to_zarr(OUTPUT_ZARR, mode="a", region=region, consolidated=False, zarr_format=2)
  print(f"[write] {ts} -> region {region}")


def main():
  parser = argparse.ArgumentParser(description="Process GFS 0.25° analysis GRIBs to Zarr.")
  parser.add_argument("--process-start", default="2021-01-01T00", help="Start datetime (inclusive), e.g., 2021-01-01T00")
  parser.add_argument("--process-end", default="2025-12-31T18", help="End datetime (inclusive), e.g., 2021-12-31T18")
  args, _ = parser.parse_known_args()

  full_times = pd.date_range("2015-01-01T00", "2030-12-31T18", freq="6h")
  dates = pd.date_range(start=args.process_start, end=args.process_end, freq="6h")

  ensure_store(full_times)

  cpus = int(os.environ.get("SLURM_CPUS_ON_NODE", os.cpu_count() or 1))
  for i in tqdm(range(0, len(dates), BATCH_SIZE), desc="GFS->Zarr"):
    chunk_times = dates[i : i + BATCH_SIZE]
    Parallel(n_jobs=cpus, backend="loky")(
        delayed(write_one_time)(ts, full_times) for ts in chunk_times
    )

# %%
if __name__ == "__main__":
  main()

# %%
