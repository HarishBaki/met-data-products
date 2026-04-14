# %%
"""
Compute global min, max, mean, and std for all variables in the combined
Geomorpho+URMA orography dataset (no time dimension; reduces over spatial dims).
"""

import xarray as xr
import numpy as np
import os

PROJECT_DIR = "/network/rit/lab/basulab/Harish/DFS"
DATA_PATH = os.path.join("/network/rit/lab/basulab/Projects/DFS/DATA", "geomorpho90m_all_vars_2p5km_urma_nys.nc")
OUT_PATH = os.path.join(PROJECT_DIR, "Geomorpho90m_downloaders", "geomorpho90m_stats.nc")

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