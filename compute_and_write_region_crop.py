#!/usr/bin/env python3
"""Compute any product's grid crop for a region, from any of three sources.

Generalized from a URMA-only, GADM-only tool. The one thing every mode
shares: given a target lat/lon extent and *any* product's full native grid
(loaded from its <product>_full_orography.nc -- see the four created this
session plus the four that already existed: CONUS404/EDDEv2/ERA5/GFS0p25),
compute that product's own index crop. Three ways to get the target extent:

  boundary  -- GADM state polygon + grid-cell padding, dimensions rounded to
               the nearest multiple of 32 (U-Net compatibility). STRICTLY
               RESERVED for URMA/RTMA (enforced -- the tool exits immediately
               for any other --product): URMA/RTMA is the one product whose
               crop actually defines the region's target domain (everything
               else gets xESMF-regridded onto it), so it's the only product
               that should have its extent derived independently from a
               state boundary rather than from URMA's own already-computed
               crop. Deriving some other product's extent this way would
               size its buffer for "covers the state" instead of "covers
               URMA's target grid with enough margin for clean regridding" --
               an easy mistake to make since both modes take a --state-like
               notion of extent, hence the enforcement rather than just a
               docs note. Use --mode reference for every other product.
  reference -- match another product's crop to an *already-cropped*
               reference grid (typically URMA's own crop, run via
               --mode boundary first) plus a halo, given as --halo-km
               (recommended -- converted to lat/lon degrees separately, so
               the buffer is genuinely equal in km on all four sides) or
               --halo-deg (equal degrees, NOT equal km away from the
               equator -- a degree of longitude shrinks by cos(latitude)).
               Two ways to point at the reference crop:
                 --reference-grid <path>          an already-cropped
                     orography .nc for the reference product (a scratch
                     file you sliced out yourself) -- the original
                     mechanism, ported from data_notes.ipynb's
                     roi_bbox_from_urma/subset_latlon_to_bbox.
                 --reference-region-config <path> --reference-product URMA
                     [--reference-tier inner|outer]
                                                   reads the reference
                     product's ALREADY-COMPUTED bbox straight out of a
                     region config YAML written by an earlier
                     --update-config run -- no scratch file needed. If that
                     product's entry has an inner/outer split (see below),
                     --reference-tier must be given explicitly (no default:
                     an ambiguous request should fail loudly, not silently
                     pick a tier).
               Inapplicable to URMA itself -- URMA IS the reference.
  bbox      -- an explicit lat/lon box, e.g. HRRR's or Ouranos's existing
               hardcoded values from data_notes.ipynb -- lets already-chosen,
               already-validated crops run through the same tool/config
               instead of staying bespoke per-product code.

Note on Ouranos specifically: its real crop mechanism is download_fx.py's
server-side THREDDS NCSS bbox subsetting, not any local index-slice -- this
tool's grid.Ouranos output is only ever a PLANNING bbox to hand to that
download, never a usable crop itself (Ouranos's rotated-pole grid means a
local index-window is a parallelogram in real lat/lon space, so its
axis-aligned bbox always requests more than it covers, and the real download
comes back a different shape/extent than predicted -- confirmed empirically,
not just theorized: 128x128 predicted locally vs 161x164 actually returned
for New Mexico). This is a known, accepted limitation, not something this
tool tries to correct -- see download_fx.py/download_ouranos.py's own
resolve_region_bbox() docs for how the bbox actually gets used.

Recommended workflow for a new region (see also each mode's docs above):
  1. --mode boundary --product URMA (or RTMA), optionally with --halo-km to
     also produce an outer/inner split (URMA-as-its-own-wide-context-source,
     e.g. for paper 4). This is the only boundary-mode run for the region.
  2. --mode reference --reference-region-config <that config> --reference-product URMA
     [--reference-tier outer] --halo-km 0, for every other product. Note
     --halo-km 0 here, not a real km buffer: interpolation safety is
     empirically a GRID-CELL question, not a km one (confirmed by direct
     xESMF testing across HRRR/ERA5/EDDEv2/Ouranos/ICON -- a fixed cell-count
     margin works uniformly across products with wildly different native
     resolutions, a fixed km number would not), so it's supplied via
     --padding 2 instead -- that's index-space padding on the TARGET
     product's OWN grid (see crop_from_bbox), which is exactly the
     "N grid cells/mesh-hops beyond the reference bbox" mechanism that was
     validated (pad=1 already clean everywhere tested; pad=2 used for an
     extra margin of safety).
  3. Once a product's crop is validated, --mode bbox with its recorded
     bbox can replay the same extent without needing the reference config.

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
  python compute_and_write_region_crop.py --product URMA --grid-source URMA/urma_full_orography.nc \\
      --mode boundary --state "New Mexico" \\
      --region-config configs/regions/New_Mexico.yaml --update-config

  # Mode 2: HRRR's crop, matched directly to URMA's already-computed New
  # Mexico crop (outer tier) via the region config -- no scratch file needed.
  # --halo-km 0: no extra real-world buffer: exactly matches URMA's outer
  # bbox. --padding 2: the empirically-validated interpolation-safety
  # margin, in HRRR's OWN grid cells.
  python compute_and_write_region_crop.py --product HRRR --grid-source HRRR/hrrr_full_orography.nc \\
      --mode reference --reference-region-config configs/regions/New_Mexico.yaml \\
      --reference-product URMA --reference-tier outer --halo-km 0 --padding 2 \\
      --region-config configs/regions/New_Mexico.yaml --update-config

  # Mode 3: reuse an already-known bbox (e.g. HRRR's existing hardcoded NYS box).
  python compute_and_write_region_crop.py --product HRRR --grid-source HRRR/hrrr_full_orography.nc \\
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
from ruamel.yaml import YAML
import matplotlib
matplotlib.use("Agg")  # headless HPC nodes have no DISPLAY
import matplotlib.pyplot as plt
from matplotlib.patches import Rectangle

# Round-trip YAML (comment/order-preserving) for --update-config specifically --
# plain yaml.safe_load/yaml.dump (used everywhere else in this file for
# read-only config loading) silently drops comments on every write, which
# wiped configs/regions/{New_York,New_Mexico}.yaml's header documentation
# across Step 4's six --update-config calls before this was caught.
_yaml_rt = YAML()
_yaml_rt.indent(mapping=2, sequence=2, offset=0)  # matches this repo's existing yaml.dump style
_yaml_rt.width = 4096  # don't line-wrap long paths

BOUNDARY_MODE_PRODUCTS = {"URMA", "RTMA"}
# --mode boundary derives an extent independently from a GADM state polygon --
# appropriate ONLY for the product whose crop actually DEFINES a region's
# target domain (everything else gets xESMF-regridded onto it). For any other
# product, "covers the state" is the wrong question -- the right one is
# "covers the reference product's grid with enough margin for clean
# regridding," which --mode reference answers. Enforced (not just documented)
# because both modes accept a --state-shaped notion of extent, making the
# mistake easy to make silently.

DEFAULT_GADM_FILE = str(Path(__file__).parent / "data" / "geospatial" / "gadm_410.gpkg")
# 2.6 GB. A plain copy of Sparse_to_dense_meteorological_variables' own
# data/geospatial/gadm_410.gpkg (not git-tracked there either -- that repo's
# data/* is wholesale gitignored, so this was always just a filesystem asset,
# never shared git history). Previously referenced in place across repos;
# copied here instead so met-data-products doesn't reach into another
# project's working tree for a static, read-only reference file. Gitignored
# here too (see .gitignore) -- same reasoning, just no longer cross-repo.


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


KM_PER_DEG_LAT = 111.32  # ~constant everywhere; a degree of longitude is shorter by cos(latitude)


def expand_bbox_km(
    bbox: tuple[float, float, float, float],
    halo_deg: float | None = None, halo_km: float | None = None,
    label: str = "bbox",
) -> tuple[float, float, float, float]:
    """Expand a (lat_min, lat_max, lon_min, lon_max) bbox by a halo. Exactly
    one of halo_deg/halo_km must be given.

    halo_km is the physically meaningful, recommended option: a degree of
    longitude is shorter than a degree of latitude away from the equator (by
    cos(latitude)), so a single halo_deg applied equally to both axes gives
    an asymmetric real-world buffer -- correct in km north/south, short in km
    east/west at non-zero latitudes. halo_km converts to lat/lon degrees
    separately, using the bbox's own mean latitude for the longitude
    conversion, so the resulting buffer is genuinely the same number of km
    on all four sides. This fix lives here, not per-product, because
    crop_from_bbox() already tests every grid kind's real lat/lon coordinates
    against whatever bbox it's given -- separable (ERA5), curvilinear
    (URMA/HRRR/EDDEv2/Ouranos), and unstructured (ICON) all inherit the
    corrected behavior automatically once the bbox itself is genuinely
    km-symmetric, with no per-grid-kind changes needed.

    halo_deg is kept only for explicit degree-based control / backward
    compatibility -- prefer halo_km.
    """
    if (halo_deg is None) == (halo_km is None):
        raise ValueError("expand_bbox_km: give exactly one of halo_km or halo_deg")

    lat_min, lat_max, lon_min, lon_max = bbox
    if halo_km is not None:
        lat_center = (lat_min + lat_max) / 2
        lat_halo_deg = halo_km / KM_PER_DEG_LAT
        lon_halo_deg = halo_km / (KM_PER_DEG_LAT * np.cos(np.radians(lat_center)))
        print(f"  {label} extent: lat=[{lat_min:.3f},{lat_max:.3f}] lon=[{lon_min:.3f},{lon_max:.3f}]  "
              f"halo={halo_km} km (lat_center={lat_center:.2f} deg -> "
              f"lat_halo={lat_halo_deg:.3f} deg, lon_halo={lon_halo_deg:.3f} deg)")
    else:
        lat_halo_deg = lon_halo_deg = halo_deg
        print(f"  {label} extent: lat=[{lat_min:.3f},{lat_max:.3f}] lon=[{lon_min:.3f},{lon_max:.3f}]  "
              f"halo={halo_deg} deg, applied equally to lat/lon -- NOT genuinely equal in km "
              f"away from the equator; prefer --halo-km")

    return (
        lat_min - lat_halo_deg, lat_max + lat_halo_deg,
        lon_min - lon_halo_deg, lon_max + lon_halo_deg,
    )


def bbox_from_reference(
    reference_grid_path: str, halo_deg: float | None = None, halo_km: float | None = None,
) -> tuple[float, float, float, float]:
    """Extent of an already-cropped reference grid (typically URMA's own crop
    for this region), expanded by a halo (see expand_bbox_km). Ported from
    data_notes.ipynb's roi_bbox_from_urma, generalized beyond CONUS404."""
    ref = load_grid(reference_grid_path)
    lon_180 = ((ref.lon + 180) % 360) - 180
    lat_min, lat_max = float(np.nanmin(ref.lat)), float(np.nanmax(ref.lat))
    lon_min, lon_max = float(np.nanmin(lon_180)), float(np.nanmax(lon_180))
    return expand_bbox_km(
        (lat_min, lat_max, lon_min, lon_max), halo_deg=halo_deg, halo_km=halo_km,
        label="Reference grid",
    )


def bbox_from_region_config(
    region_config: str, product: str, tier: str | None,
    halo_deg: float | None = None, halo_km: float | None = None,
) -> tuple[float, float, float, float]:
    """Alternative to bbox_from_reference(): reads a reference product's
    ALREADY-COMPUTED bbox straight out of a region config YAML (written by an
    earlier --update-config run), instead of requiring a manually-sliced
    scratch orography file for --reference-grid. update_region_config()
    already stores the achieved lat/lon extent for every crop it writes
    (crop["bbox_extent"], see _crop_block) -- no need to reload and re-derive
    it from the reference product's own full native grid file at all.

    Schema-aware: grid.<product> is either the flat {n0,n1,crop,bbox} schema
    or the nested {inner,outer} schema (see update_region_config's
    crop_outer docstring -- today only URMA/RTMA can have the latter, when
    boundary mode was run with a halo). For the nested case, --tier must be
    given explicitly: an ambiguous "which one did you mean" should fail
    loudly, not silently default to one.
    """
    with open(region_config) as f:
        cfg = yaml.safe_load(f)
    grid_cfg = (cfg.get("grid") or {}).get(product)
    if grid_cfg is None:
        raise ValueError(f"{region_config}: no grid.{product} entry -- run this tool for {product} first")

    if "inner" in grid_cfg:
        if tier not in ("inner", "outer"):
            raise ValueError(
                f"grid.{product} in {region_config} has an inner/outer split -- "
                f"pass --reference-tier inner or --reference-tier outer explicitly"
            )
        bbox_block = grid_cfg[tier]["bbox"]
    else:
        if tier is not None:
            raise ValueError(
                f"grid.{product} in {region_config} is a flat crop (no inner/outer split) -- "
                f"omit --reference-tier"
            )
        bbox_block = grid_cfg["bbox"]

    bbox = (bbox_block["lat_min"], bbox_block["lat_max"], bbox_block["lon_min"], bbox_block["lon_max"])
    return expand_bbox_km(
        bbox, halo_deg=halo_deg, halo_km=halo_km,
        label=f"{product}.{tier or 'flat'} bbox (from {Path(region_config).name})",
    )


def load_mesh_neighbors(path: str) -> np.ndarray:
    """neighbor_cell_index from an ICON-style mesh topology file (e.g.
    ICON-DREAM-Global_grid.nc), shape (nv, cell). NetCDF stores it 1-indexed
    (matching Fortran/CDO convention -- verified directly: min=1, max=n_cells);
    converted to 0-indexed here so it's usable directly as array indices."""
    ds = xr.open_dataset(str(path))
    if "neighbor_cell_index" not in ds:
        raise ValueError(f"{path}: expected a 'neighbor_cell_index' variable (mesh topology file)")
    return ds["neighbor_cell_index"].values - 1


def _expand_mesh_hops(mask: np.ndarray, neighbor_idx: np.ndarray, n_hops: int) -> np.ndarray:
    """BFS-expand a boolean cell mask by n_hops steps of mesh adjacency
    (neighbor_idx: 0-indexed (nv, cell), e.g. load_mesh_neighbors()'s output).
    The unstructured-grid analog of --padding's index-space expansion for
    rectangular grids -- there's no i,j structure to pad in index space, so
    "N cells of margin" becomes "N neighbor-hops of margin" instead. Confirmed
    empirically (real xESMF regridding, not just topology) that 1 hop is
    already NaN-free for ICON's DREAM mesh onto a New Mexico-sized target;
    2 hops used in practice for an extra margin of safety, matching the other
    products' validated --padding 2."""
    m = mask.copy()
    frontier = np.where(mask)[0]
    for _ in range(n_hops):
        neighbors = neighbor_idx[:, frontier].reshape(-1)
        neighbors = neighbors[neighbors >= 0]
        new_cells = neighbors[~m[neighbors]]
        if new_cells.size == 0:
            break
        m[new_cells] = True
        frontier = np.unique(new_cells)
    return m


# ---------------------------------------------------------------------------
# bbox -> index crop, generic across separable/curvilinear/unstructured
# ---------------------------------------------------------------------------
def crop_from_bbox(
    grid: Grid, bbox: tuple[float, float, float, float], padding: int, round32: bool,
    mesh_neighbors: np.ndarray | None = None, mesh_hops: int = 0,
):
    lat_min, lat_max, lon_min, lon_max = bbox
    lon_min_360, lon_max_360 = (lon_min + 360) % 360, (lon_max + 360) % 360

    if grid.kind == "unstructured":
        mask = (
            (grid.lat >= lat_min) & (grid.lat <= lat_max)
            & (grid.lon >= lon_min_360) & (grid.lon <= lon_max_360)
        )
        if int(mask.sum()) == 0:
            raise ValueError("No grid cells inside the requested bbox.")
        if mesh_hops > 0:
            if mesh_neighbors is None:
                raise ValueError("--mesh-hops > 0 requires --mesh-topology (a file with neighbor_cell_index)")
            mask = _expand_mesh_hops(mask, mesh_neighbors, mesh_hops)
        n_selected = int(mask.sum())
        return {"kind": "unstructured", "dims": grid.dims, "mask": mask, "n_selected": n_selected}

    # separable/curvilinear: lat/lon are both 1D-per-dim or both 2D over the same 2 dims.
    if grid.kind == "separable":
        lat0, lat1 = grid.lat[:, None], np.broadcast_to(grid.lat[:, None], (grid.lat.size, grid.lon.size))
        lon2d = np.broadcast_to(grid.lon[None, :], (grid.lat.size, grid.lon.size))
        lat2d = lat1
        dim0_size, dim1_size = grid.lat.size, grid.lon.size
    else:
        lat2d, lon2d = grid.lat, grid.lon
        dim0_size, dim1_size = grid.lat.shape

    in_bbox = (
        (lat2d >= lat_min) & (lat2d <= lat_max)
        & (lon2d >= lon_min_360) & (lon2d <= lon_max_360)
    )
    d0_idxs, d1_idxs = np.where(in_bbox)
    if d0_idxs.size == 0:
        raise ValueError("No grid cells inside the requested bbox.")

    d0_start = max(0, int(d0_idxs.min()) - padding)
    d1_start = max(0, int(d1_idxs.min()) - padding)
    # Exclusive end offsets (d0_start + n0 == d0_end): padding can request an
    # end past the real grid's edge (e.g. a state near CONUS's coast, where
    # URMA/HRRR simply have no more grid points beyond it) -- unclamped, this
    # silently produced an n0/n1 larger than what isel()/slicing can actually
    # deliver (numpy slicing clips rather than raising), so the achieved crop
    # was smaller than the n0/n1 recorded in config, a latent shape-mismatch
    # bug for any downstream code trusting those numbers at face value.
    d0_end_req = int(d0_idxs.max()) + padding
    d1_end_req = int(d1_idxs.max()) + padding
    d0_end = min(d0_end_req, dim0_size)
    d1_end = min(d1_end_req, dim1_size)
    clamped = d0_end_req > dim0_size or d1_end_req > dim1_size
    if clamped:
        print(f"  WARNING: padding={padding} pushes the crop past the real grid edge on "
              f"{grid.dims[0]}={dim0_size}/{grid.dims[1]}={dim1_size} -- clamped to what the grid "
              f"actually has. Achieved extent will be smaller than requested on that side.")

    n0_raw = d0_end - d0_start
    n1_raw = d1_end - d1_start
    n0 = n0_raw if not round32 else int(np.ceil(n0_raw / 32) * 32)
    n1 = n1_raw if not round32 else int(np.ceil(n1_raw / 32) * 32)

    # round32 rounds the window UP, which can itself now ask for more points
    # than remain between d0_start/d1_start and the grid's real edge --
    # clamp again and warn, rather than silently mismatching config vs data.
    n0_avail, n1_avail = dim0_size - d0_start, dim1_size - d1_start
    if n0 > n0_avail or n1 > n1_avail:
        clamped = True
        print(f"  WARNING: round32 requested n0={n0}/n1={n1} but only n0_avail={n0_avail}/"
              f"n1_avail={n1_avail} grid points remain from the start index to the grid edge -- "
              f"clamping (achieved window will not be a clean multiple of 32 on that side).")
        n0, n1 = min(n0, n0_avail), min(n1, n1_avail)

    # Real-world lat/lon extent of the SELECTED index window (post padding/
    # round32) -- not the input bbox. Needed by any product whose actual crop
    # mechanism is a download-time bbox request against a remote service
    # rather than a local isel() (Ouranos: THREDDS NCSS north/south/west/east
    # params), where the index crop itself is never applied locally.
    lat_window = lat2d[d0_start:d0_start + n0, d1_start:d1_start + n1]
    lon_window = lon2d[d0_start:d0_start + n0, d1_start:d1_start + n1]
    lon_window_180 = ((lon_window + 180) % 360) - 180
    bbox_extent = {
        "lat_min": float(np.nanmin(lat_window)), "lat_max": float(np.nanmax(lat_window)),
        "lon_min": float(np.nanmin(lon_window_180)), "lon_max": float(np.nanmax(lon_window_180)),
    }

    return {
        "kind": grid.kind, "dims": grid.dims,
        "dim0_start": d0_start, "dim1_start": d1_start,
        "n0": n0, "n1": n1,
        "bbox_extent": bbox_extent,
        "clamped": clamped,
    }


# ---------------------------------------------------------------------------
# Visual verification -- plot the cropped product's own orography against the
# requested boundaries, so a crop can be checked by eye, not just by trusting
# the printed index/extent numbers.
# ---------------------------------------------------------------------------
def load_orography(path: str) -> xr.DataArray:
    """Every <product>_full_orography.nc carries its data under 'orog'
    (standardized across all six products in this repo -- verified directly,
    not assumed)."""
    return xr.open_dataset(str(path))["orog"]


def load_state_boundary(gadm_file: str, country: str, state: str) -> gpd.GeoDataFrame:
    return gpd.read_file(str(gadm_file), where=f"NAME_0 = '{country}' AND NAME_1 = '{state}'")


def write_cropped_orography(
    grid: Grid, crop: dict, orog: xr.DataArray, out_path: Path, crop_outer: dict | None = None,
) -> None:
    """Persist a product's region-cropped orography to disk. climate-dl-downscaling's
    regridding layer (RegridderRegistry) reads ONLY a file's shape/coords as ground
    truth, never a config number -- paths.<product>_orog is opened and its .orog.shape
    IS the regridder's grid, full stop -- so this file, not met-data-products' region
    config, is what downstream actually treats as authoritative. Previously this exact
    crop was computed in-memory here (for --plot) and separately, redundantly, in each
    product's own process_and_write_to_zarr.py (for its own zarr template), discarded
    both times. This persists it once, so every downstream consumer reads the same file
    instead of each re-deriving the same slice from the full native grid.

    When crop_outer is given (URMA/RTMA's own wide-context halo -- see
    update_region_config's crop_outer docstring), the file is written at the OUTER
    extent, with the INNER tight-boundary window embedded as both a boolean 'inner_mask'
    variable and global attrs (inner_y_start/inner_x_start/inner_ny/inner_nx, relative to
    THIS FILE's own 0-based indexing, not the native full-grid indices) -- so a
    non-cropped (whole-array) training mode can still recover exactly which sub-window
    is the true state target without reaching back into met-data-products' region config
    at all. Flat crops (every other product; URMA/RTMA regions derived without a halo)
    get no mask/attrs -- the whole array IS that product's reference footprint, nothing
    to disambiguate.

    Skipped (not overwritten) if out_path already exists -- orography is static, and a
    region's crop doesn't change between runs unless deliberately re-derived (in which
    case delete the file first).
    """
    if out_path.exists():
        print(f"  Orography already present -> {out_path}")
        return
    out_path.parent.mkdir(parents=True, exist_ok=True)

    map_crop = crop_outer if crop_outer is not None else crop

    if map_crop["kind"] == "unstructured":
        mask = map_crop["mask"]
        ds = orog.isel({map_crop["dims"][0]: mask}).to_dataset(name="orog")
    else:
        d0, d1 = map_crop["dims"]
        d0s, d1s = map_crop["dim0_start"], map_crop["dim1_start"]
        n0, n1 = map_crop["n0"], map_crop["n1"]
        ds = orog.isel({d0: slice(d0s, d0s + n0), d1: slice(d1s, d1s + n1)}).to_dataset(name="orog")
        if crop_outer is not None:
            iy0, ix0 = crop["dim0_start"] - d0s, crop["dim1_start"] - d1s
            iny, inx = crop["n0"], crop["n1"]
            mask = np.zeros((n0, n1), dtype=bool)
            mask[iy0:iy0 + iny, ix0:ix0 + inx] = True
            ds["inner_mask"] = ((d0, d1), mask)
            ds.attrs.update(inner_y_start=iy0, inner_x_start=ix0, inner_ny=iny, inner_nx=inx)

    ds.load().to_netcdf(out_path)
    print(f"  Orography written -> {out_path}")


def _crop_extent_and_map_data(grid: Grid, crop: dict, orog: xr.DataArray):
    """(lat_min, lat_max, lon_min, lon_max, lon_for_plot, lat_for_plot, orog_values)
    for one crop -- shared by the unstructured/separable/curvilinear branches
    so plot_crop() can call this once for inner and once for outer."""
    if crop["kind"] == "unstructured":
        mask = crop["mask"]
        lon_180 = ((grid.lon + 180) % 360) - 180
        lat_min, lat_max = float(grid.lat[mask].min()), float(grid.lat[mask].max())
        lon_min, lon_max = float(lon_180[mask].min()), float(lon_180[mask].max())
        return lat_min, lat_max, lon_min, lon_max

    d0_start, d1_start = crop["dim0_start"], crop["dim1_start"]
    n0, n1 = crop["n0"], crop["n1"]
    if grid.kind == "separable":
        orog_crop = orog.isel({grid.dims[0]: slice(d0_start, d0_start + n0),
                                grid.dims[1]: slice(d1_start, d1_start + n1)})
        lat_crop = grid.lat[d0_start:d0_start + n0]
        lon_crop = ((grid.lon[d1_start:d1_start + n1] + 180) % 360) - 180
    else:
        orog_crop = orog.isel({grid.dims[0]: slice(d0_start, d0_start + n0),
                                grid.dims[1]: slice(d1_start, d1_start + n1)})
        lat_crop = grid.lat[d0_start:d0_start + n0, d1_start:d1_start + n1]
        lon_crop = ((grid.lon[d0_start:d0_start + n0, d1_start:d1_start + n1] + 180) % 360) - 180
    be = crop["bbox_extent"]
    return be["lat_min"], be["lat_max"], be["lon_min"], be["lon_max"], lon_crop, lat_crop, orog_crop.values


def plot_crop(
    grid: Grid, crop: dict, grid_source: str, product: str, region_id: str,
    out_path: Path, boundary_gdf: gpd.GeoDataFrame | None = None,
    crop_outer: dict | None = None,
) -> None:
    """Plot the actual cropped orography (not just the index numbers), with
    the selected bbox extent and (if available) the real GADM state boundary
    overlaid, so the crop can be visually sanity-checked before trusting it.

    When crop_outer is given (URMA's paper-4 wide-context halo -- see
    update_region_config), the map background is drawn from the OUTER crop
    (the wider view), with both the inner (tight, GADM-boundary) and outer
    (halo-inclusive) extents drawn as separate rectangles on top -- otherwise
    only crop's own extent is shown, as before."""
    orog = load_orography(grid_source)
    fig, ax = plt.subplots(figsize=(8, 7))

    map_crop = crop_outer if crop_outer is not None else crop

    if map_crop["kind"] == "unstructured":
        mask = map_crop["mask"]
        lon_180 = ((grid.lon + 180) % 360) - 180
        # Full mesh as faint context, selected cells on top in color -- shows
        # where the crop sits within the whole native mesh, not just in isolation.
        ax.scatter(lon_180, grid.lat, s=1, c="0.85", linewidths=0)
        sel = ax.scatter(
            lon_180[mask], grid.lat[mask], s=4,
            c=np.asarray(orog.values).reshape(-1)[mask], cmap="terrain",
        )
        fig.colorbar(sel, ax=ax, label="orog [m]", shrink=0.8)
        map_lat_min, map_lat_max, map_lon_min, map_lon_max = _crop_extent_and_map_data(grid, map_crop, orog)
        title_extra = f"{map_crop['n_selected']} of {grid.lat.size} cells selected"
    else:
        map_lat_min, map_lat_max, map_lon_min, map_lon_max, lon_plot, lat_plot, orog_plot = (
            _crop_extent_and_map_data(grid, map_crop, orog)
        )
        mesh = ax.pcolormesh(lon_plot, lat_plot, orog_plot, shading="auto", cmap="terrain")
        fig.colorbar(mesh, ax=ax, label="orog [m]", shrink=0.8)
        n0, n1 = map_crop["n0"], map_crop["n1"]
        title_extra = f"{map_crop['dims'][0]}x{map_crop['dims'][1]} = {n0}x{n1}"

    if crop_outer is not None:
        inner_ext = _crop_extent_and_map_data(grid, crop, orog)[:4]
        ax.add_patch(Rectangle(
            (inner_ext[2], inner_ext[0]), inner_ext[3] - inner_ext[2], inner_ext[1] - inner_ext[0],
            fill=False, edgecolor="blue", linewidth=1.5, linestyle="-", label="inner (tight) extent",
        ))
        ax.add_patch(Rectangle(
            (map_lon_min, map_lat_min), map_lon_max - map_lon_min, map_lat_max - map_lat_min,
            fill=False, edgecolor="red", linewidth=1.5, linestyle="--", label="outer (halo) extent",
        ))
        title_extra += f"  (inner {crop['n0']}x{crop['n1']})"
    else:
        ax.add_patch(Rectangle(
            (map_lon_min, map_lat_min), map_lon_max - map_lon_min, map_lat_max - map_lat_min,
            fill=False, edgecolor="red", linewidth=1.5, linestyle="--", label="selected crop extent",
        ))

    if boundary_gdf is not None and len(boundary_gdf) > 0:
        # dissolve first -- GADM may return multiple sub-state (e.g. county) features,
        # and plotting each one's boundary separately draws internal admin lines
        # instead of one clean outer state outline.
        boundary_gdf.dissolve().boundary.plot(ax=ax, color="black", linewidth=1.2, label="state boundary")

    pad = 0.5
    ax.set_xlim(map_lon_min - pad, map_lon_max + pad)
    ax.set_ylim(map_lat_min - pad, map_lat_max + pad)
    ax.set_xlabel("longitude")
    ax.set_ylabel("latitude")
    ax.set_title(f"{product} -- {region_id}\n{title_extra}")
    ax.legend(loc="upper right", fontsize=8)

    out_path.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(out_path, dpi=150, bbox_inches="tight")
    plt.close(fig)
    print(f"  Plot saved -> {out_path}")


# ---------------------------------------------------------------------------
# Region config I/O
# ---------------------------------------------------------------------------
def _crop_block(crop: dict) -> dict:
    """The n0/n1/crop/bbox block for one (non-unstructured) crop -- factored
    out so inner and outer can each get one, identically shaped."""
    d0, d1 = crop["dims"]
    block = {
        "n0": int(crop["n0"]), "n1": int(crop["n1"]),
        "crop": {f"{d0}_start": int(crop["dim0_start"]), f"{d1}_start": int(crop["dim1_start"])},
        "bbox": {k: round(v, 6) for k, v in crop["bbox_extent"].items()},
    }
    if crop.get("clamped"):
        # Achieved extent is smaller than what --padding/--round32 asked for --
        # the grid's real edge got in the way (see crop_from_bbox). n0/n1/bbox
        # above are already the ACHIEVED values; this just flags that they
        # differ from what was requested, so it isn't mistaken for a clean result.
        block["clamped_to_grid_edge"] = True
    return block


def update_region_config(
    region_config: str, product: str, crop: dict, mask_out_dir: Path | None,
    crop_outer: dict | None = None,
):
    """crop_outer is only meaningful for products that need to serve as their
    OWN wide-context source, not just a target others regrid onto -- today
    that's URMA specifically (paper 4's "coarsened URMA" wide-LR source needs
    URMA's own crop to carry a halo beyond its tight, GADM-boundary training
    footprint, the same way the other five products already carry one beyond
    URMA's footprint). When given, grid.<product> becomes {type, dims, inner,
    outer} instead of the flat {type, dims, n0, n1, crop, bbox} -- kept flat
    when crop_outer is None so every already-committed entry (New York's,
    and every reference-mode product here) is completely unaffected."""
    region_path = Path(region_config)
    with open(region_path) as f:
        cfg = _yaml_rt.load(f)
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
    elif crop_outer is not None:
        d0, d1 = crop["dims"]
        cfg["grid"][product] = {
            "type": crop["kind"],
            "dims": [d0, d1],
            "inner": _crop_block(crop),
            "outer": _crop_block(crop_outer),
        }
    else:
        d0, d1 = crop["dims"]
        cfg["grid"][product] = {
            "type": crop["kind"],
            "dims": [d0, d1],
            **_crop_block(crop),
        }

    with open(region_path, "w") as f:
        _yaml_rt.dump(cfg, f)
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

    # mode=reference (required there); also optional for --mode boundary, to additionally
    # compute an "outer" halo-inclusive crop alongside the tight "inner" one -- see
    # update_region_config's crop_outer docstring for why (URMA-as-its-own-wide-source).
    p.add_argument("--reference-grid", default=None, help="Path to the reference product's ALREADY-CROPPED orography (typically URMA's)")
    p.add_argument(
        "--reference-region-config", default=None,
        help="Alternative to --reference-grid: a region config YAML (already written by an "
             "earlier --update-config run) to read the reference product's bbox from directly -- "
             "no scratch orography file needed. Requires --reference-product; exactly one of "
             "--reference-grid/--reference-region-config must be given for --mode reference."
    )
    p.add_argument(
        "--reference-product", default=None,
        help="Which grid.<product> entry to read from --reference-region-config (e.g. URMA)."
    )
    p.add_argument(
        "--reference-tier", default=None, choices=["inner", "outer"],
        help="Required only when the reference product's config entry has an inner/outer split "
             "(see update_region_config) -- e.g. URMA's own halo-inclusive outer crop, for other "
             "products regridding onto it with wide-context margin. Omit for a flat crop entry."
    )
    p.add_argument(
        "--halo-km", type=float, default=None,
        help="Halo in kilometers, converted to lat/lon degrees separately so the buffer is "
             "genuinely equal in km on all four sides (recommended -- see bbox_from_reference). "
             "Required (with --halo-deg as the alternative) for --mode reference; optional for "
             "--mode boundary, where it additionally produces an outer/inner split."
    )
    p.add_argument(
        "--halo-deg", type=float, default=None,
        help="Halo in degrees, applied equally to lat/lon -- NOT equal in km away from the "
             "equator (a degree of longitude shrinks by cos(latitude)). Prefer --halo-km."
    )

    # mode=bbox
    p.add_argument("--lat-min", type=float, default=None)
    p.add_argument("--lat-max", type=float, default=None)
    p.add_argument("--lon-min", type=float, default=None)
    p.add_argument("--lon-max", type=float, default=None)

    p.add_argument("--padding", type=int, default=10, help="Grid-cell padding around the bbox (default: 10). No-op for unstructured (ICON-style) grids -- see --mesh-hops.")
    p.add_argument(
        "--mesh-hops", type=int, default=0,
        help="Unstructured (ICON-style) grids only: neighbor-hop margin beyond the bbox-selected "
             "cells, via --mesh-topology's connectivity -- the mesh analog of --padding (there's "
             "no i,j index space to pad). Empirically validated: 1 hop is already NaN-free onto a "
             "New-Mexico-sized target; 2 used for extra margin, matching --padding 2 elsewhere. "
             "Default 0 (no expansion, previous behavior) -- must be set explicitly."
    )
    p.add_argument(
        "--mesh-topology", default=None,
        help="Path to a mesh topology file with a neighbor_cell_index variable (e.g. "
             "ICON-DREAM-Global_grid.nc) -- required when --mesh-hops > 0."
    )
    p.add_argument("--round32", action="store_true", help="Round dimensions up to the nearest multiple of 32 (default for --mode boundary)")
    p.add_argument("--region-config", default=None)
    p.add_argument("--update-config", action="store_true")
    p.add_argument("--mask-out-dir", default=None, help="Directory to save unstructured (ICON-style) cell masks")

    p.add_argument(
        "--plot", action="store_true",
        help="Plot the cropped product's own orography with the selected extent (and, if "
             "--state is given, the real GADM state boundary) overlaid, for visual "
             "verification. Saved as <region-config-dir>/region_maps/<region_id>/<product>.png "
             "by default -- one subfolder per region, keeping the configs/regions/ tree itself "
             "uncluttered."
    )
    p.add_argument(
        "--plot-out-dir", default=None,
        help="Directory to save the --plot figure (as <product>.png directly inside it, no "
             "region subfolder). Defaults to <region-config-dir>/region_maps/<region_id>/ if "
             "--region-config is given; required otherwise."
    )
    args = p.parse_args()

    if args.mode == "boundary" and args.product.upper() not in BOUNDARY_MODE_PRODUCTS:
        raise SystemExit(
            f"ERROR: --mode boundary is reserved for URMA/RTMA (got --product={args.product}).\n"
            f"  URMA/RTMA's crop defines the region's target domain; every other product should\n"
            f"  be derived from it via --mode reference (see --reference-region-config), so its\n"
            f"  buffer accounts for xESMF regridding onto URMA's grid rather than an independent\n"
            f"  GADM-boundary computation. Use --mode bbox instead if you already have an\n"
            f"  explicit, pre-validated lat/lon box for a non-reference product."
        )

    print(f"Loading {args.product}'s full native grid from {args.grid_source} ...")
    grid = load_grid(args.grid_source)
    print(f"  kind={grid.kind}  dims={grid.dims}  shape={grid.lat.shape}  "
          f"lat=[{grid.lat.min():.2f},{grid.lat.max():.2f}]  lon=[{grid.lon.min():.2f},{grid.lon.max():.2f}]")

    outer_bbox = None
    if args.mode == "boundary":
        if not args.state:
            raise ValueError("--mode boundary requires --state")
        print(f"Deriving bbox from GADM boundary for {args.country}/{args.state} ...")
        bbox = bbox_from_gadm(args.gadm_file, args.country, args.state)
        round32 = True if not args.round32 else args.round32  # default on for boundary mode
        if args.halo_km is not None or args.halo_deg is not None:
            if (args.halo_deg is None) == (args.halo_km is None):
                raise ValueError("give exactly one of --halo-km or --halo-deg, not both")
            outer_bbox = expand_bbox_km(
                bbox, halo_deg=args.halo_deg, halo_km=args.halo_km, label="GADM boundary",
            )
    elif args.mode == "reference":
        if (args.reference_grid is None) == (args.reference_region_config is None):
            raise ValueError(
                "--mode reference requires exactly one of --reference-grid or "
                "--reference-region-config"
            )
        if (args.halo_deg is None) == (args.halo_km is None):
            raise ValueError("--mode reference requires exactly one of --halo-km or --halo-deg")
        if args.reference_grid:
            print(f"Deriving bbox from reference grid {args.reference_grid} ...")
            bbox = bbox_from_reference(args.reference_grid, halo_deg=args.halo_deg, halo_km=args.halo_km)
        else:
            if not args.reference_product:
                raise ValueError("--reference-region-config requires --reference-product")
            print(
                f"Deriving bbox from grid.{args.reference_product}"
                f"{'.' + args.reference_tier if args.reference_tier else ''} "
                f"in {args.reference_region_config} ..."
            )
            bbox = bbox_from_region_config(
                args.reference_region_config, args.reference_product, args.reference_tier,
                halo_deg=args.halo_deg, halo_km=args.halo_km,
            )
        round32 = args.round32  # off unless explicitly requested
    else:  # bbox
        if None in (args.lat_min, args.lat_max, args.lon_min, args.lon_max):
            raise ValueError("--mode bbox requires --lat-min/--lat-max/--lon-min/--lon-max")
        bbox = (args.lat_min, args.lat_max, args.lon_min, args.lon_max)
        round32 = args.round32  # off unless explicitly requested

    if args.mesh_hops > 0 and not args.mesh_topology:
        raise ValueError("--mesh-hops > 0 requires --mesh-topology")
    mesh_neighbors = load_mesh_neighbors(args.mesh_topology) if args.mesh_topology else None

    crop = crop_from_bbox(grid, bbox, args.padding, round32, mesh_neighbors=mesh_neighbors, mesh_hops=args.mesh_hops)
    crop_outer = (
        crop_from_bbox(grid, outer_bbox, args.padding, round32, mesh_neighbors=mesh_neighbors, mesh_hops=args.mesh_hops)
        if outer_bbox is not None else None
    )

    print()
    if crop["kind"] == "unstructured":
        print(f"  {crop['n_selected']} of {grid.lat.size} cells selected")
    else:
        d0, d1 = crop["dims"]
        print(f"  inner: {d0}_start = {crop['dim0_start']}   n{d0} = {crop['n0']}")
        print(f"  inner: {d1}_start = {crop['dim1_start']}   n{d1} = {crop['n1']}")
        be = crop["bbox_extent"]
        print(f"  inner bbox extent: lat=[{be['lat_min']:.3f},{be['lat_max']:.3f}]  lon=[{be['lon_min']:.3f},{be['lon_max']:.3f}]")
        if crop_outer is not None:
            print(f"  outer: {d0}_start = {crop_outer['dim0_start']}   n{d0} = {crop_outer['n0']}")
            print(f"  outer: {d1}_start = {crop_outer['dim1_start']}   n{d1} = {crop_outer['n1']}")
            beo = crop_outer["bbox_extent"]
            print(f"  outer bbox extent: lat=[{beo['lat_min']:.3f},{beo['lat_max']:.3f}]  lon=[{beo['lon_min']:.3f},{beo['lon_max']:.3f}]")

    if args.update_config:
        if not args.region_config:
            raise ValueError("--update-config requires --region-config")
        mask_dir = Path(args.mask_out_dir) if args.mask_out_dir else Path(args.region_config).parent
        update_region_config(args.region_config, args.product, crop, mask_dir, crop_outer=crop_outer)

        with open(args.region_config) as f:
            region_cfg_for_orog = yaml.safe_load(f)
        data_root = region_cfg_for_orog.get("data_root")
        region_tag = region_cfg_for_orog.get("region_tag")
        if data_root and region_tag:
            orog_out_path = Path(data_root) / f"{args.product}_{region_tag}" / "cropped_orography.nc"
            orog = load_orography(args.grid_source)
            write_cropped_orography(grid, crop, orog, orog_out_path, crop_outer=crop_outer)
        else:
            print(
                f"  Skipping cropped orography write -- {args.region_config} has no "
                f"data_root/region_tag set yet."
            )

    if args.plot:
        if not args.plot_out_dir and not args.region_config:
            raise ValueError("--plot requires --plot-out-dir or --region-config")

        if args.region_config:
            with open(args.region_config) as f:
                region_id = yaml.safe_load(f).get("region_id", Path(args.region_config).stem)
            # One subfolder per region under region_maps/, {product}.png inside it --
            # keeps region_maps/ itself uncluttered as more products/regions accumulate.
            default_plot_dir = Path(args.region_config).parent / "region_maps" / region_id
        else:
            region_id = "region"
            default_plot_dir = None

        plot_dir = Path(args.plot_out_dir) if args.plot_out_dir else default_plot_dir
        boundary_gdf = load_state_boundary(args.gadm_file, args.country, args.state) if args.state else None
        out_path = plot_dir / f"{args.product}.png"
        plot_crop(grid, crop, args.grid_source, args.product, region_id, out_path, boundary_gdf, crop_outer=crop_outer)


if __name__ == "__main__":
    main()
