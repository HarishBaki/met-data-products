#!/usr/bin/env python3
"""Compute any product's grid crop for a region, from any of three sources.

Generalized from a URMA-only, GADM-only tool. The one thing every mode
shares: given a target lat/lon extent and *any* product's full native grid
(loaded from its <product>_full_orography.nc -- see the four created this
session plus the four that already existed: CONUS404/EDDEv2/ERA5/GFS0p25),
compute that product's own index crop. Three ways to get the target extent:

  boundary  -- GADM state polygon + grid-cell padding, dimensions rounded to
               the nearest multiple of 32 (U-Net compatibility). This is the
               only mode that should be run against URMA: URMA is the one
               product whose crop actually defines the region's target
               domain (everything else gets xESMF-regridded onto it).
  reference -- match another product's crop to an *already-cropped*
               reference grid (typically URMA's own crop, run via
               --mode boundary first) plus a degree-based halo. Ported from
               data_notes.ipynb's roi_bbox_from_urma/subset_latlon_to_bbox,
               which had only ever been hand-run once, for CONUS404.
               Inapplicable to URMA itself -- URMA IS the reference.
  bbox      -- an explicit lat/lon box, e.g. HRRR's or Ouranos's existing
               hardcoded values from data_notes.ipynb -- lets already-chosen,
               already-validated crops run through the same tool/config
               instead of staying bespoke per-product code.

Grid loading is generic across all three modes: every product's
<product>_full_orography.nc already has latitude/longitude coordinates
(Step 1 of this plan standardized that), but the underlying grid shape still
falls into one of three kinds, auto-detected here:
  separable    -- 1D latitude(latitude), longitude(longitude) (ERA5, GFS0p25)
  curvilinear  -- 2D latitude/longitude over two other named dims, e.g. (y,x)
                  for URMA/HRRR/EDDEv2/CONUS404 or (rlat,rlon) for Ouranos
  unstructured -- 1D latitude/longitude over a single dim with no grid
                  structure at all (ICON's cell dim) -- crop is a boolean
                  mask, not a rectangular index window, and gets saved to
                  its own file rather than inlined in region config YAML.

Usage:
  # Mode 1: URMA's own crop, from a real state boundary (as before).
  python compute_region_crop.py --product URMA --grid-source URMA/urma_full_orography.nc \\
      --mode boundary --state "New Mexico" \\
      --region-config configs/regions/New_Mexico.yaml --update-config

  # Mode 2: HRRR's crop, matched to URMA's already-computed New Mexico crop.
  python compute_region_crop.py --product HRRR --grid-source HRRR/hrrr_full_orography.nc \\
      --mode reference --reference-grid <URMA's cropped orography for this region> \\
      --region-config configs/regions/New_Mexico.yaml --update-config

  # Mode 3: reuse an already-known bbox (e.g. HRRR's existing hardcoded NYS box).
  python compute_region_crop.py --product HRRR --grid-source HRRR/hrrr_full_orography.nc \\
      --mode bbox --lat-min 38 --lat-max 48 --lon-min -82 --lon-max -68 \\
      --region-config configs/regions/New_York.yaml --update-config
"""
from __future__ import annotations

import argparse
from pathlib import Path

import numpy as np
import xarray as xr
import geopandas as gpd
import yaml

DEFAULT_GADM_FILE = (
    "/network/rit/lab/basulab/Harish/ResearchOS/Projects/Active/"
    "Sparse-to-grided-deep-interpolation/git_repos/Sparse_to_dense_meteorological_variables/"
    "data/geospatial/gadm_410.gpkg"
)
# 2.76 GB, already validated for New York/New Mexico in the Sparse_to_dense project --
# referenced in place rather than duplicated into this repo. Static/read-only reference
# data, not project-specific code.


# ---------------------------------------------------------------------------
# Generic grid loading -- every product's <product>_full_orography.nc, once
# Step 1 standardized them, needs no product-specific read code any more.
# ---------------------------------------------------------------------------
class Grid:
    """lat/lon (+ kind + dim names) for one product's full native grid."""

    def __init__(self, lat: np.ndarray, lon: np.ndarray, dims: tuple[str, ...], kind: str):
        self.lat = lat
        self.lon = lon  # always normalized to [0, 360]
        self.dims = dims  # 2 dims for separable/curvilinear, 1 for unstructured
        self.kind = kind  # "separable" | "curvilinear" | "unstructured"


def load_grid(path: str) -> Grid:
    ds = xr.open_dataset(str(path))
    if "latitude" not in ds.coords or "longitude" not in ds.coords:
        raise ValueError(f"{path}: expected latitude/longitude coordinates, found {list(ds.coords)}")
    lat_da, lon_da = ds.latitude, ds.longitude
    lon = (lon_da.values + 360) % 360  # normalize to [0, 360] regardless of source convention

    if lat_da.ndim == 1 and lat_da.dims == (lat_da.name,) and lon_da.dims == (lon_da.name,):
        # Separable grid: lat/lon ARE the dims (ERA5, GFS0p25).
        return Grid(lat_da.values, lon, dims=(lat_da.dims[0], lon_da.dims[0]), kind="separable")
    if lat_da.ndim == 2:
        # Curvilinear/rotated grid: lat/lon vary over two OTHER named dims
        # (y,x for URMA/HRRR/EDDEv2/CONUS404; rlat,rlon for Ouranos).
        return Grid(lat_da.values, lon, dims=lat_da.dims, kind="curvilinear")
    if lat_da.ndim == 1:
        # Unstructured mesh: lat/lon over a single dim with no grid structure (ICON's cell).
        return Grid(lat_da.values, lon, dims=lat_da.dims, kind="unstructured")
    raise ValueError(f"{path}: unrecognized lat/lon shape {lat_da.dims} (ndim={lat_da.ndim})")


# ---------------------------------------------------------------------------
# Obtaining a target lat/lon bbox -- the three modes
# ---------------------------------------------------------------------------
def bbox_from_gadm(gadm_file: str, country: str, state: str) -> tuple[float, float, float, float]:
    """Raw (unbuffered) state bbox, in [-180,180] lon. Padding is applied later,
    in index space, not here -- same design as before."""
    gdf = gpd.read_file(str(gadm_file), where=f"NAME_0 = '{country}' AND NAME_1 = '{state}'")
    if len(gdf) == 0:
        raise ValueError(f"No GADM features found for {country}/{state} in {gadm_file}")
    min_lon, min_lat, max_lon, max_lat = gdf.total_bounds
    print(f"  GADM features: {len(gdf)}  raw bbox (lon/lat): {gdf.total_bounds}")
    return min_lat, max_lat, min_lon, max_lon


def bbox_from_reference(reference_grid_path: str, halo_deg: float) -> tuple[float, float, float, float]:
    """Extent of an already-cropped reference grid (typically URMA's own crop
    for this region), expanded by a degree-based halo. Ported from
    data_notes.ipynb's roi_bbox_from_urma, generalized beyond CONUS404."""
    ref = load_grid(reference_grid_path)
    lon_180 = ((ref.lon + 180) % 360) - 180
    lat_min, lat_max = float(np.nanmin(ref.lat)), float(np.nanmax(ref.lat))
    lon_min, lon_max = float(np.nanmin(lon_180)), float(np.nanmax(lon_180))
    print(f"  Reference grid extent: lat=[{lat_min:.3f},{lat_max:.3f}] lon=[{lon_min:.3f},{lon_max:.3f}]  "
          f"halo={halo_deg} deg")
    return lat_min - halo_deg, lat_max + halo_deg, lon_min - halo_deg, lon_max + halo_deg


# ---------------------------------------------------------------------------
# bbox -> index crop, generic across separable/curvilinear/unstructured
# ---------------------------------------------------------------------------
def crop_from_bbox(grid: Grid, bbox: tuple[float, float, float, float], padding: int, round32: bool):
    lat_min, lat_max, lon_min, lon_max = bbox
    lon_min_360, lon_max_360 = (lon_min + 360) % 360, (lon_max + 360) % 360

    if grid.kind == "unstructured":
        mask = (
            (grid.lat >= lat_min) & (grid.lat <= lat_max)
            & (grid.lon >= lon_min_360) & (grid.lon <= lon_max_360)
        )
        n_selected = int(mask.sum())
        if n_selected == 0:
            raise ValueError("No grid cells inside the requested bbox.")
        return {"kind": "unstructured", "dims": grid.dims, "mask": mask, "n_selected": n_selected}

    # separable/curvilinear: lat/lon are both 1D-per-dim or both 2D over the same 2 dims.
    if grid.kind == "separable":
        lat0, lat1 = grid.lat[:, None], np.broadcast_to(grid.lat[:, None], (grid.lat.size, grid.lon.size))
        lon2d = np.broadcast_to(grid.lon[None, :], (grid.lat.size, grid.lon.size))
        lat2d = lat1
    else:
        lat2d, lon2d = grid.lat, grid.lon

    in_bbox = (
        (lat2d >= lat_min) & (lat2d <= lat_max)
        & (lon2d >= lon_min_360) & (lon2d <= lon_max_360)
    )
    d0_idxs, d1_idxs = np.where(in_bbox)
    if d0_idxs.size == 0:
        raise ValueError("No grid cells inside the requested bbox.")

    d0_start = max(0, int(d0_idxs.min()) - padding)
    d1_start = max(0, int(d1_idxs.min()) - padding)
    d0_end = int(d0_idxs.max()) + padding
    d1_end = int(d1_idxs.max()) + padding
    n0 = (d0_end - d0_start) if not round32 else int(np.ceil((d0_end - d0_start) / 32) * 32)
    n1 = (d1_end - d1_start) if not round32 else int(np.ceil((d1_end - d1_start) / 32) * 32)

    return {
        "kind": grid.kind, "dims": grid.dims,
        "dim0_start": d0_start, "dim1_start": d1_start,
        "n0": n0, "n1": n1,
    }


# ---------------------------------------------------------------------------
# Region config I/O
# ---------------------------------------------------------------------------
def update_region_config(region_config: str, product: str, crop: dict, mask_out_dir: Path | None):
    region_path = Path(region_config)
    cfg = yaml.safe_load(region_path.read_text())
    cfg.setdefault("grid", {})

    if crop["kind"] == "unstructured":
        if mask_out_dir is None:
            raise ValueError("Unstructured (ICON-style) crop requires --mask-out-dir to save the cell mask.")
        mask_path = mask_out_dir / f"{cfg.get('region_id', 'region')}_{product}_cell_mask.nc"
        xr.DataArray(crop["mask"], dims=crop["dims"], name="mask").to_netcdf(mask_path)
        cfg["grid"][product] = {
            "type": "unstructured",
            "dims": list(crop["dims"]),
            "n_selected": crop["n_selected"],
            "cell_mask_file": str(mask_path),
        }
        print(f"  Cell mask saved -> {mask_path}  ({crop['n_selected']} cells selected)")
    else:
        d0, d1 = crop["dims"]
        cfg["grid"][product] = {
            "type": crop["kind"],
            "dims": [d0, d1],
            "n0": int(crop["n0"]), "n1": int(crop["n1"]),
            "crop": {f"{d0}_start": int(crop["dim0_start"]), f"{d1}_start": int(crop["dim1_start"])},
        }

    region_path.write_text(yaml.dump(cfg, default_flow_style=False, sort_keys=False))
    print(f"  Config updated -> {region_path}  (grid.{product})")


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------
def main():
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--product", required=True, help="Product name, used as the grid.<product> key in region config")
    p.add_argument("--grid-source", required=True, help="Path to that product's <product>_full_orography.nc")
    p.add_argument("--mode", required=True, choices=["boundary", "reference", "bbox"])

    # mode=boundary
    p.add_argument("--gadm-file", default=DEFAULT_GADM_FILE)
    p.add_argument("--country", default="United States")
    p.add_argument("--state", default=None)

    # mode=reference
    p.add_argument("--reference-grid", default=None, help="Path to the reference product's ALREADY-CROPPED orography (typically URMA's)")
    p.add_argument("--halo-deg", type=float, default=1.0)

    # mode=bbox
    p.add_argument("--lat-min", type=float, default=None)
    p.add_argument("--lat-max", type=float, default=None)
    p.add_argument("--lon-min", type=float, default=None)
    p.add_argument("--lon-max", type=float, default=None)

    p.add_argument("--padding", type=int, default=10, help="Grid-cell padding around the bbox (default: 10)")
    p.add_argument("--round32", action="store_true", help="Round dimensions up to the nearest multiple of 32 (default for --mode boundary)")
    p.add_argument("--region-config", default=None)
    p.add_argument("--update-config", action="store_true")
    p.add_argument("--mask-out-dir", default=None, help="Directory to save unstructured (ICON-style) cell masks")
    args = p.parse_args()

    print(f"Loading {args.product}'s full native grid from {args.grid_source} ...")
    grid = load_grid(args.grid_source)
    print(f"  kind={grid.kind}  dims={grid.dims}  shape={grid.lat.shape}  "
          f"lat=[{grid.lat.min():.2f},{grid.lat.max():.2f}]  lon=[{grid.lon.min():.2f},{grid.lon.max():.2f}]")

    if args.mode == "boundary":
        if not args.state:
            raise ValueError("--mode boundary requires --state")
        print(f"Deriving bbox from GADM boundary for {args.country}/{args.state} ...")
        bbox = bbox_from_gadm(args.gadm_file, args.country, args.state)
        round32 = True if not args.round32 else args.round32  # default on for boundary mode
    elif args.mode == "reference":
        if not args.reference_grid:
            raise ValueError("--mode reference requires --reference-grid")
        print(f"Deriving bbox from reference grid {args.reference_grid} ...")
        bbox = bbox_from_reference(args.reference_grid, args.halo_deg)
        round32 = args.round32  # off unless explicitly requested
    else:  # bbox
        if None in (args.lat_min, args.lat_max, args.lon_min, args.lon_max):
            raise ValueError("--mode bbox requires --lat-min/--lat-max/--lon-min/--lon-max")
        bbox = (args.lat_min, args.lat_max, args.lon_min, args.lon_max)
        round32 = args.round32  # off unless explicitly requested

    crop = crop_from_bbox(grid, bbox, args.padding, round32)

    print()
    if crop["kind"] == "unstructured":
        print(f"  {crop['n_selected']} of {grid.lat.size} cells selected")
    else:
        d0, d1 = crop["dims"]
        print(f"  {d0}_start = {crop['dim0_start']}   n{d0} = {crop['n0']}")
        print(f"  {d1}_start = {crop['dim1_start']}   n{d1} = {crop['n1']}")

    if args.update_config:
        if not args.region_config:
            raise ValueError("--update-config requires --region-config")
        mask_dir = Path(args.mask_out_dir) if args.mask_out_dir else Path(args.region_config).parent
        update_region_config(args.region_config, args.product, crop, mask_dir)


if __name__ == "__main__":
    main()
