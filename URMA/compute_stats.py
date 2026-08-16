# %%
#!/usr/bin/env python
"""Compute per-variable min/max/mean/std (plus a log-transformed variant for tp)
over a date range of a region's URMA zarr store -- used by DL_downscaling's
input/target normalization (see dataloader.py's build_transform(), which reads
this file from paths.urma_stats in configs/regions/{region}.yaml).

Usage:
    python compute_stats.py --region New_Mexico --start 2018-01-01 --end 2023-12-31
"""
import argparse
import sys
from pathlib import Path

import xarray as xr
import numpy as np

BOOTSTRAP_ROOT = Path(__file__).resolve().parents[1]
if str(BOOTSTRAP_ROOT) not in sys.path:
  sys.path.insert(0, str(BOOTSTRAP_ROOT))

from repo_utils import find_repo_root, load_region_vars

PROJECT_DIR = find_repo_root(__file__)


def main():
  parser = argparse.ArgumentParser(description=__doc__)
  parser.add_argument("--region", required=True, help="e.g. New_York, New_Mexico")
  parser.add_argument("--start", default="2018-01-01")
  parser.add_argument("--end", default="2023-12-31")
  args = parser.parse_args()

  region_vars = load_region_vars(args.region)
  data_root = region_vars["data_root"]
  region_tag = region_vars["region_tag"]
  zarr_store = f"{data_root}/URMA_{region_tag}/URMA_{region_tag}.zarr"
  out_path = PROJECT_DIR / "URMA" / f"urma_stats_{region_tag}_{args.start[:4]}_{args.end[:4]}.nc"

  print(f"Opening Zarr store: {zarr_store}")
  ds = xr.open_zarr(zarr_store, chunks="auto", consolidated=False)
  print(f"Selecting time range {args.start} to {args.end}")
  ds_train = ds.sel(time=slice(args.start, args.end))

  stats = {}
  for var in ds_train.data_vars:
    print(f"Computing stats for {var}")
    da = ds_train[var]
    dims = tuple(dim for dim in da.dims)  # reduce over all dims
    stats[f"{var}_min"] = da.min(dim=dims, skipna=True)
    stats[f"{var}_max"] = da.max(dim=dims, skipna=True)
    stats[f"{var}_mean"] = da.mean(dim=dims, skipna=True)
    stats[f"{var}_std"] = da.std(dim=dims, skipna=True)
    if var == "tp":
      da_log = np.log10(1.0 + da)
      prefix = f"log_{var}"
      stats[f"{prefix}_min"] = da_log.min(dim=dims, skipna=True)
      stats[f"{prefix}_max"] = da_log.max(dim=dims, skipna=True)
      stats[f"{prefix}_mean"] = da_log.mean(dim=dims, skipna=True)
      stats[f"{prefix}_std"] = da_log.std(dim=dims, skipna=True)

  print("Combining and writing output")
  ds_out = xr.Dataset(stats)
  ds_out.attrs["source"] = zarr_store
  ds_out.attrs["time_range"] = f"{args.start} to {args.end}"
  ds_out.to_netcdf(str(out_path))
  print(f"Wrote stats to {out_path}")


# %%
if __name__ == "__main__":
  main()

# %%
