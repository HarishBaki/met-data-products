# %%
"""
Combine all regridded Geomorpho90m variables with URMA orography into one NetCDF.
- Reads per-variable files from 2p5km_urma_nys_files (expects named variables).
- Adds URMA orography as 'orog'.
"""

import glob
import os

import xarray as xr

PROJECT_DIR = "/network/rit/lab/basulab/Harish/DFS"
GEOM_DIR = os.path.join(PROJECT_DIR, "Geomorpho90m_downloaders")
GEOM_FILES_GLOB = os.path.join(GEOM_DIR, "2p5km_urma_nys_files", "geomorpho90m_*_2p5km_urma_nys.nc")
URMA_OROG = os.path.join(PROJECT_DIR, "urma_nys_orography.nc")
OUT_PATH = os.path.join('/network/rit/lab/basulab/Projects/DFS/DATA', "geomorpho90m_all_vars_2p5km_urma_nys.nc")
OUT_PATH_TMP = OUT_PATH + ".tmp"  # write-then-replace to avoid open-file conflicts
FILL_VALUE = 0.0  # neutral fill to remove NaNs per-variable before merge

# %%
if __name__ == "__main__":
  # %%
  files = sorted(glob.glob(GEOM_FILES_GLOB))
  if not files:
    raise FileNotFoundError(f"No geomorpho files found matching {GEOM_FILES_GLOB}")

  datasets = []
  for f in files:
    ds = xr.open_dataset(f)
    ds = ds.fillna(FILL_VALUE)
    datasets.append(ds)

  merged = xr.merge(datasets, compat="override")

  # Add URMA orography (rename to 'orog' if present)
  orog_ds = xr.open_dataset(URMA_OROG)
  if "orog" not in orog_ds and len(orog_ds.data_vars) == 1:
    only_var = list(orog_ds.data_vars)[0]
    orog_ds = orog_ds.rename({only_var: "orog"})
  merged = xr.merge([merged, orog_ds], compat="override")

  # %%
  # Updating the attributes
  merged.attrs.update({
    "title": "Geomorpho90m geomorphometric variables aggregated to 2.5 km and regridded to URMA (NYS domain)",
    "source": "Geomorpho90m derived from MERIT-DEM (Amatulli et al., 2020) and URMA orography",
    "reference": "Amatulli et al. (2020), Scientific Data, https://doi.org/10.1038/s41597-020-0479-6",
    "original_resolution": "3 arc-second (~90 m)",
    "processing_steps": (
        "Tiles mosaicked using GDAL VRT; clipped to NYS domain; "
        "reprojected to EPSG:5070; aggregated to 2.5 km; "
        "reprojected to WGS84; bilinearly regridded to URMA grid using xESMF"
    ),
    "projection": "WGS84 (EPSG:4326), URMA grid",
    "institution": "University at Albany, ASRC",
  })
  # %%
  VAR_ATTRS = {
    "aspect": {
    "long_name": "Terrain aspect",
    "description": "Azimuth of maximum downslope direction derived from DEM",
    "units": "degrees",
    "notes": "Circular variable; primarily provided for completeness",
  },
  "aspect-sine": {
    "long_name": "Sine of terrain aspect",
    "description": "Sine-transformed terrain aspect to avoid angular discontinuity",
    "units": "1",
  },
  "aspect-cosine": {
    "long_name": "Cosine of terrain aspect",
    "description": "Cosine-transformed terrain aspect to avoid angular discontinuity",
    "units": "1",
  },
  "eastness": {
    "long_name": "Terrain eastness",
    "description": "Eastward component of terrain orientation (sin(aspect))",
    "units": "1",
  },
  "northness": {
    "long_name": "Terrain northness",
    "description": "Northward component of terrain orientation (cos(aspect))",
    "units": "1",
  },
  "orog": {
    "long_name": "Surface elevation",
    "description": "URMA surface orography",
    "units": "m",
    "source": "URMA analysis",
  },
  "elev-stdev": {
      "long_name": "Standard deviation of elevation",
      "description": "Local elevation variability within a moving window",
      "units": "m",
  },
  "tpi": {
      "long_name": "Topographic Position Index",
      "description": "Difference between a cell elevation and the mean elevation of its neighborhood",
      "units": "m",
  },
  "tri": {
      "long_name": "Terrain Ruggedness Index",
      "description": "Mean absolute difference between a cell and its surrounding neighbors",
      "units": "m",
  },
  "vrm": {
      "long_name": "Vector Ruggedness Measure",
      "description": "Terrain ruggedness based on variation in surface normals",
      "units": "1",
  },
  "slope": {
    "long_name": "Terrain slope",
    "description": "Maximum rate of change in elevation",
    "units": "degrees",
  },
  "roughness": {
      "long_name": "Terrain roughness",
      "description": "Difference between maximum and minimum elevation in a local neighborhood",
      "units": "m",
  },
  "rough-magnitude": {
      "long_name": "Multiscale roughness magnitude",
      "description": "Magnitude component of multiscale terrain roughness",
      "units": "m",
  },
  "rough-scale": {
      "long_name": "Multiscale roughness scale",
      "description": "Characteristic scale associated with maximum terrain roughness",
      "units": "m",
  },
  "dx": {
    "long_name": "First derivative of elevation in x-direction",
    "description": "Partial derivative of elevation with respect to longitude",
    "units": "m/m",
  },
  "dy": {
      "long_name": "First derivative of elevation in y-direction",
      "description": "Partial derivative of elevation with respect to latitude",
      "units": "m/m",
  },
  "dxx": {
      "long_name": "Second derivative of elevation in x-direction",
      "description": "Second-order partial derivative of elevation",
      "units": "1/m",
  },
  "dyy": {
      "long_name": "Second derivative of elevation in y-direction",
      "description": "Second-order partial derivative of elevation",
      "units": "1/m",
  },
  "dxy": {
      "long_name": "Cross derivative of elevation",
      "description": "Mixed second-order derivative of elevation",
      "units": "1/m",
  },
  "pcurv": {
      "long_name": "Profile curvature",
      "description": "Curvature in the direction of maximum slope",
      "units": "1/m",
  },
  "tcurv": {
      "long_name": "Total curvature",
      "description": "Combined measure of profile and plan curvature",
      "units": "1/m",
  },
  "geom": {
      "long_name": "Gaussian curvature",
      "description": "Product of the two principal curvatures of the surface",
      "units": "1/m^2",
  },
  "cti": {
    "long_name": "Compound Topographic Index",
    "description": "Wetness index combining local slope and upstream contributing area",
    "units": "1",
  },
  "spi": {
      "long_name": "Stream Power Index",
      "description": "Index proportional to erosive power of overland flow",
      "units": "1",
  },
  "convergence": {
      "long_name": "Terrain convergence index",
      "description": "Measure of terrain convergence or divergence",
      "units": "degrees",
  },
  "dev-magnitude": {
    "long_name": "Terrain deviation magnitude",
    "description": "Magnitude of deviation from local planar surface",
    "units": "m",
  },
  "dev-scale": {
      "long_name": "Terrain deviation scale",
      "description": "Characteristic scale at which terrain deviation is maximized",
      "units": "m",
  },
  }
  # %%
  for var, attrs in VAR_ATTRS.items():
    if var in merged:
        merged[var].attrs.update(attrs)
        merged[var].attrs.update({
            "source": "Geomorpho90m (Amatulli et al., 2020)",
            "processing": "Aggregated to 2.5 km and regridded to URMA",
        })
  # %%
  # Write to temp then atomically replace target to avoid issues if file is open elsewhere
  merged.to_netcdf(OUT_PATH_TMP)
  os.replace(OUT_PATH_TMP, OUT_PATH)
  print(f"Wrote combined dataset to {OUT_PATH}")
