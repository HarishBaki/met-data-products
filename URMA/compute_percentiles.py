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
OUT_PATH = PROJECT_DIR / "URMA" / f"urma_percentiles_{START[:4]}_{END[:4]}.nc"
PERCENTILES = np.arange(1, 101, dtype=np.int32)


def main():
  print(f"Opening Zarr store: {ZARR_STORE}")
  ds = xr.open_zarr(ZARR_STORE, chunks="auto")
  print(f"Selecting time range {START} to {END}")
  ds_train = ds.sel(time=slice(START, END))

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
  ds_out.attrs["source"] = ZARR_STORE
  ds_out.attrs["time_range"] = f"{START} to {END}"
  ds_out.to_netcdf(str(OUT_PATH))
  print(f"Wrote percentiles to {OUT_PATH}")

# %%
if __name__ == "__main__":
  main()

# %%
