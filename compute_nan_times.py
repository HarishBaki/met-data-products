#!/usr/bin/env python3
"""
Audit missing (NaN) timestamps in a product's zarr store(s) for a region, one
nan_times_<var>.nc per variable, written next to the store(s) it came from.

Single script for all 6 products (URMA, HRRR, EDDEv2, ERA5, ICON-DREAM-Global,
Ouranos) instead of one per-product copy, and region-parameterized via the
same configs/regions/{region}.yaml -> load_region_vars() every product's own
process_and_write_to_zarr*.py already uses -- see repo_utils.py.

Every actual *.zarr store under the product's region root is discovered via
glob rather than hardcoded (EDDEv2's run types, Ouranos's catalog rows x
frequencies, ERA5-ARCO's internal zarr groups), so this stays correct as
products add/remove run types, catalog rows, or groups without needing
changes here -- confirmed against real data: EDDEv2 correctly finds all 3
run-type stores, ERA5-ARCO correctly discovers its 'sl' group (with 'pl'/'ml'
picked up automatically once enabled) without either being named explicitly
anywhere in this script.

Usage:
    python compute_nan_times.py --product ICON-DREAM-Global --region New_Mexico
"""
from __future__ import annotations

import argparse
from collections import Counter
from pathlib import Path
from typing import Optional

import xarray as xr
import zarr

from repo_utils import load_region_vars

# Maps each product's CLI name (matching its repo directory name) to its
# region-root subdirectory pattern, relative to data_root -- the one piece of
# per-product knowledge this script needs, mirrored from each product's own
# resolve_region_output()-equivalent (see URMA/HRRR/EDDEv2/ERA5-ARCO/ICON's
# process_and_write_to_zarr*.py and Ouranos/download_ouranos.py).
PRODUCT_ROOT_PREFIX = {
    "URMA": "URMA_{region_tag}",
    "HRRR": "HRRR_{region_tag}",
    "EDDEv2": "EDDEv2_{region_tag}/hourly/WRF-MPI",
    "ERA5": "ERA5_{region_tag}",
    "ICON-DREAM-Global": "ICON_DREAM_Global_{region_tag}",
    "Ouranos": "Ouranos_{region_tag}",
}


def discover_stores(product: str, region: str) -> list[Path]:
    region_vars = load_region_vars(region)
    data_root = region_vars["data_root"]
    region_tag = region_vars["region_tag"]
    if not data_root:
        raise ValueError(f"configs/regions/{region}.yaml has no data_root set yet.")

    root = Path(data_root) / PRODUCT_ROOT_PREFIX[product].format(region_tag=region_tag)
    if not root.exists():
        raise FileNotFoundError(
            f"{root} does not exist -- has {product} been processed for region {region!r} yet?"
        )
    # rglob catches EDDEv2's per-run-type stores and Ouranos's per (catalog
    # row, frequency) stores, wherever they land, without knowing the layout
    # in advance. ".zarr.sync" lock directories don't end in ".zarr" so they
    # never match.
    stores = sorted(p for p in root.rglob("*.zarr") if p.is_dir())
    if not stores:
        raise FileNotFoundError(f"No *.zarr stores found under {root}")
    return stores


def store_groups(store: Path) -> list[Optional[str]]:
    """List zarr sub-groups inside a store (e.g. ERA5-ARCO's sl/pl/ml), or
    [None] (root group) if it has none."""
    zg = zarr.open_group(str(store), mode="r")
    groups = list(zg.group_keys())
    return groups if groups else [None]


def compute_nan_times_for_group(store: Path, group: Optional[str], output_dir: Path) -> None:
    label = f"{store.name}[{group}]" if group else store.name
    print(f"=== {label} ===", flush=True)
    ds = xr.open_zarr(str(store), group=group, consolidated=False)
    if "time" not in ds.coords:
        print(f"  [skip] no 'time' coord in {label}", flush=True)
        return

    year_counts = ds["time"].groupby("time.year").count().compute()
    if year_counts.size == 0:
        print("  time coverage by year: none (no timestamps)", flush=True)
    else:
        print("  time coverage by year:", flush=True)
        for year, count in zip(year_counts["year"].values, year_counts.values):
            print(f"    {int(year)}: {int(count)}", flush=True)

    output_dir.mkdir(parents=True, exist_ok=True)
    for var_name in ds.data_vars:
        da = ds[var_name]
        if "time" not in da.dims:
            continue
        reduce_dims = tuple(d for d in da.dims if d != "time")
        nan_mask = da.isnull().any(dim=reduce_dims) if reduce_dims else da.isnull()
        nan_times = ds["time"].where(nan_mask).dropna("time").compute()
        print(f"  {var_name}: {len(nan_times)} NaN timestamps", flush=True)
        if len(nan_times):
            gap_year_counts = nan_times.groupby("time.year").count()
            for year, count in zip(gap_year_counts["year"].values, gap_year_counts.values):
                print(f"    {int(year)}: {int(count)}", flush=True)

        out_name = f"nan_times_{var_name}.nc" if group is None else f"nan_times_{group}_{var_name}.nc"
        out_path = output_dir / out_name
        nan_times.to_netcdf(str(out_path))
        print(f"  [done] -> {out_path}", flush=True)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Audit NaN timestamps in a product's zarr store(s) for a region."
    )
    parser.add_argument("--product", required=True, choices=sorted(PRODUCT_ROOT_PREFIX))
    parser.add_argument("--region", required=True, help="e.g. New_York, New_Mexico")
    args = parser.parse_args()

    stores = discover_stores(args.product, args.region)
    print(f"Found {len(stores)} store(s) for product={args.product} region={args.region}:")
    for s in stores:
        print(f"  {s}")
    print(flush=True)

    # Multiple stores sharing the same parent directory (EDDEv2's 3 run
    # types) get one nan_times_<stem> dir each, matching the old per-product
    # scripts' convention; a store alone in its parent (URMA/HRRR/ERA5/ICON,
    # and typically each of Ouranos's per-row stores) just gets "nan_times".
    parent_counts = Counter(s.parent for s in stores)
    for store in stores:
        dirname = f"nan_times_{store.stem}" if parent_counts[store.parent] > 1 else "nan_times"
        output_dir = store.parent / dirname
        for group in store_groups(store):
            compute_nan_times_for_group(store, group, output_dir)


if __name__ == "__main__":
    main()
