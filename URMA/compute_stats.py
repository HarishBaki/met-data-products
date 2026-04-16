# %%
#!/usr/bin/env python
import sys
from pathlib import Path

import xarray as xr
import numpy as np

BOOTSTRAP_ROOT = Path(__file__).resolve().parents[1]
if str(BOOTSTRAP_ROOT) not in sys.path:
  sys.path.insert(0, str(BOOTSTRAP_ROOT))

from repo_utils import find_repo_root

PROJECT_DIR = find_repo_root(__file__)
ZARR_STORE = "/network/rit/lab/basulab/Projects/DFS/DATA/URMA_NYS/URMA_NYS.zarr"
START = "2018-01-01"
END = "2023-12-31"
OUT_PATH = PROJECT_DIR / "URMA" / f"urma_stats_{START[:4]}_{END[:4]}.nc"

def main():
  print(f"Opening Zarr store: {ZARR_STORE}")
  ds = xr.open_zarr(ZARR_STORE, chunks="auto")
  print(f"Selecting time range {START} to {END}")
  ds_train = ds.sel(time=slice(START, END))

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
  ds_out.attrs["source"] = ZARR_STORE
  ds_out.attrs["time_range"] = f"{START} to {END}"
  ds_out.to_netcdf(str(OUT_PATH))
  print(f"Wrote stats to {OUT_PATH}")

# %%
if __name__ == "__main__":
  main()

# %%
