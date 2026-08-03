#!/usr/bin/env python3
"""Compute a region's grid crop (x_start, y_start, nx, ny) against URMA's
native grid from an actual GADM state boundary, instead of hand-picking one.

Ported from Sparse_to_dense_meteorological_variables's
scripts/data_processing/generate_static.py (--auto-grid path), which already
validated this approach for New York/New Mexico in that project. Reasoning
is unchanged: the state's raw (unbuffered) bounding box is used to find
which native-grid pixels the state can plausibly touch, then padded by a
grid-cell margin (not the same thing as boundary.buffer_m, which is a
meters-based buffer used only for the fine-grained per-pixel state mask
generated elsewhere -- this script only computes the crop window, not the
mask). --auto-grid also rounds nx/ny up to the nearest multiple of 32
(common divisibility requirement for U-Net-style downsampling stacks).

Usage:
    python compute_region_crop.py --state "New Mexico" \\
        --grib /network/rit/lab/basulab/RAW_DATA/URMA/20250101/urma2p5.t00z.2dvaranl_ndfd.grb2_wexp \\
        --auto-grid

    # Write/update configs/regions/New_Mexico.yaml with the computed values:
    python compute_region_crop.py --state "New Mexico" --region-config configs/regions/New_Mexico.yaml \\
        --grib /network/rit/lab/basulab/RAW_DATA/URMA/20250101/urma2p5.t00z.2dvaranl_ndfd.grb2_wexp \\
        --auto-grid --update-config
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


def load_full_grib_latlon(grib_path: str, shortname: str = "10si") -> tuple[np.ndarray, np.ndarray]:
    """Full (uncropped) URMA native grid lat/lon -- must be opened with no y/x
    slicing, unlike every product's own process_and_write_to_zarr.py."""
    ds = xr.open_dataset(
        str(grib_path), engine="cfgrib",
        backend_kwargs={"indexpath": "", "filter_by_keys": {"shortName": shortname}},
    )
    return ds.latitude.values, ds.longitude.values


def load_boundary_gdf(gadm_file: str, country: str, state: str) -> gpd.GeoDataFrame:
    gdf = gpd.read_file(str(gadm_file), where=f"NAME_0 = '{country}' AND NAME_1 = '{state}'")
    if len(gdf) == 0:
        raise ValueError(f"No GADM features found for {country}/{state} in {gadm_file}")
    return gdf


def _state_bbox_indices(full_lat, full_lon, gdf, padding):
    min_lon, min_lat, max_lon, max_lat = gdf.total_bounds
    # URMA lon is [0, 360]; GADM is [-180, 180] -- convert bbox accordingly.
    min_lon_360 = (min_lon + 360) % 360
    max_lon_360 = (max_lon + 360) % 360
    in_bbox = (
        (full_lat >= min_lat) & (full_lat <= max_lat)
        & (full_lon >= min_lon_360) & (full_lon <= max_lon_360)
    )
    y_idxs, x_idxs = np.where(in_bbox)
    if len(y_idxs) == 0:
        raise ValueError(
            f"No URMA grid cells inside the bounding box of "
            f"'{gdf.iloc[0]['NAME_1']}'. Check country/state names."
        )
    return y_idxs, x_idxs


def compute_auto_crop(full_lat, full_lon, gdf, padding):
    """x_start/y_start only -- nx/ny taken from wherever the caller already has them."""
    y_idxs, x_idxs = _state_bbox_indices(full_lat, full_lon, gdf, padding)
    x_start = max(0, int(x_idxs.min()) - padding)
    y_start = max(0, int(y_idxs.min()) - padding)
    return x_start, y_start


def compute_auto_grid(full_lat, full_lon, gdf, padding):
    """All four grid params, nx/ny rounded up to the nearest multiple of 32."""
    y_idxs, x_idxs = _state_bbox_indices(full_lat, full_lon, gdf, padding)
    x_start = max(0, int(x_idxs.min()) - padding)
    y_start = max(0, int(y_idxs.min()) - padding)
    x_end = int(x_idxs.max()) + padding
    y_end = int(y_idxs.max()) + padding
    nx = int(np.ceil((x_end - x_start) / 32) * 32)
    ny = int(np.ceil((y_end - y_start) / 32) * 32)
    return x_start, y_start, nx, ny


def main():
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--state", required=True, help="GADM NAME_1, e.g. 'New York', 'New Mexico'")
    p.add_argument("--country", default="United States", help="GADM NAME_0 (default: United States)")
    p.add_argument("--gadm-file", default=DEFAULT_GADM_FILE)
    p.add_argument("--grib", required=True, help="Any raw URMA GRIB2 file (full native grid, unsliced)")
    p.add_argument("--padding", type=int, default=10, help="Grid-cell padding around state bbox (default: 10)")
    p.add_argument("--auto-crop", action="store_true", help="Compute x_start/y_start only (nx/ny given via --nx/--ny)")
    p.add_argument("--nx", type=int, default=None, help="Used with --auto-crop")
    p.add_argument("--ny", type=int, default=None, help="Used with --auto-crop")
    p.add_argument("--auto-grid", action="store_true", help="Compute x_start/y_start/nx/ny (default mode)")
    p.add_argument("--region-config", default=None, help="configs/regions/{Region}.yaml to read boundary from / write grid to")
    p.add_argument("--update-config", action="store_true", help="Write computed grid params back into --region-config")
    args = p.parse_args()

    if not args.auto_crop:
        args.auto_grid = True  # default

    print(f"Loading full URMA native grid from {args.grib} ...")
    full_lat, full_lon = load_full_grib_latlon(args.grib)
    print(f"  Full domain shape: {full_lat.shape}  lat=[{full_lat.min():.2f},{full_lat.max():.2f}]  "
          f"lon=[{full_lon.min():.2f},{full_lon.max():.2f}]")

    print(f"Loading GADM boundary for {args.country}/{args.state} ...")
    gdf = load_boundary_gdf(args.gadm_file, args.country, args.state)
    print(f"  Features: {len(gdf)}  raw bbox (lon/lat): {gdf.total_bounds}")

    if args.auto_crop:
        if args.nx is None or args.ny is None:
            raise ValueError("--auto-crop requires --nx/--ny (or use --auto-grid to compute them too)")
        x_start, y_start = compute_auto_crop(full_lat, full_lon, gdf, args.padding)
        nx, ny = args.nx, args.ny
    else:
        x_start, y_start, nx, ny = compute_auto_grid(full_lat, full_lon, gdf, args.padding)

    print()
    print(f"  x_start = {x_start}   nx = {nx}")
    print(f"  y_start = {y_start}   ny = {ny}")
    print(f"  (crop covers native-grid indices y[{y_start}:{y_start+ny}] x[{x_start}:{x_start+nx}])")

    if args.update_config:
        if not args.region_config:
            raise ValueError("--update-config requires --region-config")
        region_path = Path(args.region_config)
        cfg = yaml.safe_load(region_path.read_text())
        cfg.setdefault("grid", {})
        cfg["grid"]["nx"] = int(nx)
        cfg["grid"]["ny"] = int(ny)
        cfg["grid"].setdefault("crop", {})
        cfg["grid"]["crop"]["x_start"] = int(x_start)
        cfg["grid"]["crop"]["y_start"] = int(y_start)
        region_path.write_text(yaml.dump(cfg, default_flow_style=False, sort_keys=False))
        print(f"  Config updated -> {region_path}")


if __name__ == "__main__":
    main()
