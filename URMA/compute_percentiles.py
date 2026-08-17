# %%
#!/usr/bin/env python
"""Compute per-variable 1st-99th percentiles over a date range of a region's URMA
zarr store -- used by DL_downscaling's InverseWeightedLoss (see stochastic_cgan.yaml's
training.loss), which reads this from paths.urma_percentiles in
configs/regions/{region}.yaml.

Usage:
    python compute_percentiles.py --region New_Mexico --start 2018-01-01 --end 2023-12-31
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
PERCENTILES = np.arange(1, 101, dtype=np.int32)


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
  out_path = PROJECT_DIR / "URMA" / f"urma_percentiles_{region_tag}_{args.start[:4]}_{args.end[:4]}.nc"

  print(f"Opening Zarr store: {zarr_store}")
  ds = xr.open_zarr(zarr_store, chunks="auto", consolidated=False)
  print(f"Selecting time range {args.start} to {args.end}")
  ds_train = ds.sel(time=slice(args.start, args.end))

  percentiles = {}
  q = PERCENTILES / 100.0
  for var in ds_train.data_vars:
    print(f"Computing percentiles for {var}")
    da = ds_train[var]
    if "time" not in da.dims:
      raise ValueError(f"{var} has no 'time' dimension; cannot compute percentiles over time.")
    q_da = da.quantile(q, dim="time", skipna=True).rename({"quantile": "percentile"})
    q_da = q_da.assign_coords(percentile=PERCENTILES)
    percentiles[var] = q_da

  print("Combining and writing output")
  ds_out = xr.Dataset(percentiles)
  ds_out.attrs["source"] = zarr_store
  ds_out.attrs["time_range"] = f"{args.start} to {args.end}"
  ds_out.to_netcdf(str(out_path))
  print(f"Wrote percentiles to {out_path}")

# %%
if __name__ == "__main__":
  main()

# %%
