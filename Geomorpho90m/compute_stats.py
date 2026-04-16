# %%
"""
Compute global min, max, mean, and std for all variables in the combined
Geomorpho+URMA orography dataset (no time dimension; reduces over spatial dims).
"""

import sys
from pathlib import Path

import xarray as xr
import numpy as np
import os

BOOTSTRAP_ROOT = Path(__file__).resolve().parents[1]
if str(BOOTSTRAP_ROOT) not in sys.path:
  sys.path.insert(0, str(BOOTSTRAP_ROOT))

from repo_utils import find_repo_root

PROJECT_DIR = find_repo_root(__file__)
DATA_PATH = os.path.join("/network/rit/lab/basulab/Projects/DFS/DATA", "geomorpho90m_all_vars_2p5km_urma_nys.nc")
OUT_PATH = os.path.join(PROJECT_DIR, "Geomorpho90m", "geomorpho90m_stats.nc")

# %%
if __name__ == "__main__":
  # %%
  ds = xr.open_dataset(DATA_PATH)
  stats = {}
  for var in ds.data_vars:
    da = ds[var]
    dims = tuple(da.dims)  # reduce over all dims (spatial only)
    stats[f"{var}_min"] = da.min(dim=dims, skipna=True)
    stats[f"{var}_max"] = da.max(dim=dims, skipna=True)
    stats[f"{var}_mean"] = da.mean(dim=dims, skipna=True)
    stats[f"{var}_std"] = da.std(dim=dims, skipna=True)
  # %%
  ds_out = xr.Dataset(stats)
  ds_out.attrs["source"] = DATA_PATH
  ds_out.to_netcdf(OUT_PATH)
  print(f"Wrote spatial stats to {OUT_PATH}")
