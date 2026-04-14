# %%
import os
import sys

import numpy as np
import rioxarray as rxr
import xarray as xr
import xesmf as xe
from rasterio.enums import Resampling

PROJECT_DIR = "/network/rit/lab/basulab/Harish/DFS"

LAT_MIN = 38
LAT_MAX = 48
LON_MIN = -82
LON_MAX = -68

# %%
if __name__ == "__main__":
  # %%
  # Allow interactive/IPython runs without argparse errors
  var = "slope"
  vrt_path = os.path.join(PROJECT_DIR, "Geomorpho90m_downloaders", "vrt_files", f"{var}_90m.vrt")

  # If CLI args are present (outside IPython), honor them
  if len(sys.argv) > 1 and not sys.argv[0].endswith("ipykernel_launcher.py"):
    import argparse
    parser = argparse.ArgumentParser(description="Process Geomorpho 90m tiles to URMA grid.")
    parser.add_argument("variable", nargs="?", default=var, help="Variable name (e.g., slope)")
    parser.add_argument("--vrt", default=None, help="Path to VRT for the variable")
    args, _ = parser.parse_known_args()
    var = args.variable
    if args.vrt:
      vrt_path = args.vrt
    else:
      vrt_path = os.path.join(PROJECT_DIR, "Geomorpho90m_downloaders", "vrt_files", f"{var}_90m.vrt")

  if not os.path.exists(vrt_path):
    raise FileNotFoundError(f"VRT not found for {var}: {vrt_path}")
  
  # %%
  da = rxr.open_rasterio(vrt_path, masked=True).squeeze()
  da = da.sel(
      x=slice(LON_MIN, LON_MAX),
      y=slice(LAT_MAX, LAT_MIN)  # y is descending
  )
  src_attrs = da.attrs.copy()
  # %%
  da_m = da.rio.reproject(
      "EPSG:5070",
      resampling=Resampling.bilinear
  )

  dx = abs(da_m.rio.resolution()[0])  # ~90
  factor = int(round(2500 / dx))
  print(f"[{var}] Aggregation factor: {factor}")

  da_2p5km = da_m.coarsen(
      x=factor,
      y=factor,
      boundary="trim"
  ).mean()

  da_2p5km_ll = da_2p5km.rio.reproject(
      "EPSG:4326",
      resampling=Resampling.bilinear
  )

  lon2d, lat2d = np.meshgrid(
      da_2p5km_ll.x.values,
      da_2p5km_ll.y.values
  )

  da_2p5km_ll = da_2p5km_ll.assign_coords(
      lon=(("y", "x"), lon2d),
      lat=(("y", "x"), lat2d)
  )

  urma_path = os.path.join(PROJECT_DIR, "urma_nys_orography.nc")
  urma_nys = xr.open_dataset(urma_path)
  regridder = xe.Regridder(
      da_2p5km_ll,
      urma_nys,
      method="bilinear",
      reuse_weights=False
  )

  da_urma = regridder(da_2p5km_ll)
  da_urma.name = var
  # carry over useful attributes if present
  var_attrs = {
      "long_name": src_attrs.get("long_name", var),
      "units": src_attrs.get("units", src_attrs.get("unit", None)),
  }
  # remove None entries
  var_attrs = {k: v for k, v in var_attrs.items() if v is not None}
  da_urma.attrs.update(var_attrs)

  # %%
  out_dir = os.path.join(PROJECT_DIR, "Geomorpho90m_downloaders", "2p5km_urma_nys_files")
  os.makedirs(out_dir, exist_ok=True)
  out_path = os.path.join(out_dir, f"geomorpho90m_{var}_2p5km_urma_nys.nc")
  da_urma.to_netcdf(out_path)
  print(f"[{var}] Wrote {out_path}")
