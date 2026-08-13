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

Each variable's NaN scan runs in its own worker process (joblib/loky, same
pattern as every product's own process_and_write_to_zarr*.py), one per
--cpus-per-task. Needed because scanning a store's full history is a
genuinely large read (confirmed in production: a single URMA scan read 655GB
off NFS) and xarray/dask's default .compute() left 7 of 8 requested CPUs
idle doing it serially -- checked live on the node (98.9% idle, 0.0% iowait)
while it ran. This isn't about avoiding contention with other jobs, it's
about actually using the CPUs this job already asks for.

Usage:
    python compute_nan_times.py --product ICON-DREAM-Global --region New_Mexico
"""
from __future__ import annotations

import argparse
import os
from collections import Counter
from pathlib import Path
from typing import Optional

import xarray as xr
import zarr
from joblib import Parallel, delayed

from data_utils.zarr_io import get_slurm_cpus
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
    # Walks to find EDDEv2's per-run-type stores and Ouranos's per (catalog
    # row, frequency) stores, wherever they land, without knowing the layout
    # in advance -- but pruned with os.walk() rather than Path.rglob("*.zarr"):
    # rglob doesn't stop at a match, it keeps recursing *inside* every
    # matched .zarr directory too, walking its entire internal chunk-file
    # tree (a store can be tens of thousands of tiny files) looking for
    # further nested ".zarr" matches that can never exist. For New Mexico's
    # 1-3 stores/product that overhead was too small to notice; against New
    # York's 7-19 stores/product it hung for 6+ minutes on what should be an
    # instant directory listing (confirmed via `stat`/`ls`/`df` on the same
    # mount all returning immediately while the unpruned walk was still
    # stuck). Pruning at each ".zarr" boundary -- treating it as a leaf, not
    # descending further -- avoids ever touching a store's internals here.
    # ".zarr.sync" lock directories don't end in ".zarr" so they're never
    # matched or pruned into either way. "_to_<target>_<method>.zarr" stores
    # (e.g. "URMA_NYS_to_EDDE_LR_bilinear.zarr") are downstream regridded
    # outputs, not this product's own raw processing output -- excluded from
    # the result, still pruned from traversal like any other ".zarr" match.
    stores = []
    for dirpath, dirnames, _ in os.walk(root):
        matched = [d for d in dirnames if d.endswith(".zarr")]
        for d in matched:
            if "_to_" not in Path(d).stem:
                stores.append(Path(dirpath) / d)
        dirnames[:] = [d for d in dirnames if d not in matched]
    stores.sort()
    if not stores:
        raise FileNotFoundError(f"No *.zarr stores found under {root}")
    return stores


def store_groups(store: Path) -> list[Optional[str]]:
    """List zarr sub-groups inside a store (e.g. ERA5-ARCO's sl/pl/ml), or
    [None] (root group) if it has none."""
    zg = zarr.open_group(str(store), mode="r")
    groups = list(zg.group_keys())
    return groups if groups else [None]


def print_time_coverage(store: Path, group: Optional[str]) -> None:
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


def scan_one_var(store: Path, group: Optional[str], var_name: str, output_dir: Path) -> str:
    """Runs in its own worker process -- opens its own handle on the store
    rather than sharing one across processes, same as every product's own
    per-(var, month/year) worker functions.

    Writes two separate registries per var, since they answer different
    questions for different consumers:
    - nan_times_<var>.nc ("any" -- at least one pixel in the domain is NaN):
      the training-time exclusion list DL_downscaling/dataloader.py's
      _collect_missing_times reads -- correctly conservative there, since a
      single bad pixel still makes a sample unfit to train on.
    - nan_times_<var>_fullynan.nc ("all" -- every pixel in the domain is
      NaN): the genuinely-nothing-there population, for consumers that need
      to know whether *any* usable data exists at a timestamp at all (e.g.
      data_prep/regrid_to_urma_zarr.py's _month_is_complete -- a month
      containing a source hour with even one valid pixel is not the same
      problem as one with zero, and conflating the two here previously
      caused the wrong file to get reused for that purpose).
    """
    ds = xr.open_zarr(str(store), group=group, consolidated=False)
    da = ds[var_name]
    reduce_dims = tuple(d for d in da.dims if d != "time")
    isnull = da.isnull()
    any_nan_mask = isnull.any(dim=reduce_dims) if reduce_dims else isnull
    all_nan_mask = isnull.all(dim=reduce_dims) if reduce_dims else isnull
    any_nan_times = ds["time"].where(any_nan_mask).dropna("time").compute()
    all_nan_times = ds["time"].where(all_nan_mask).dropna("time").compute()

    lines = [f"  {var_name}: {len(any_nan_times)} any-NaN timestamps, {len(all_nan_times)} fully-NaN timestamps"]
    if len(any_nan_times):
        gap_year_counts = any_nan_times.groupby("time.year").count()
        for year, count in zip(gap_year_counts["year"].values, gap_year_counts.values):
            lines.append(f"    any-NaN {int(year)}: {int(count)}")
    if len(all_nan_times):
        gap_year_counts = all_nan_times.groupby("time.year").count()
        for year, count in zip(gap_year_counts["year"].values, gap_year_counts.values):
            lines.append(f"    fully-NaN {int(year)}: {int(count)}")

    base_name = f"nan_times_{var_name}" if group is None else f"nan_times_{group}_{var_name}"
    any_out_path = output_dir / f"{base_name}.nc"
    all_out_path = output_dir / f"{base_name}_fullynan.nc"
    any_nan_times.to_netcdf(str(any_out_path))
    all_nan_times.to_netcdf(str(all_out_path))
    lines.append(f"  [done] -> {any_out_path}")
    lines.append(f"  [done] -> {all_out_path}")
    return "\n".join(lines)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Audit NaN timestamps in a product's zarr store(s) for a region."
    )
    parser.add_argument("--product", required=True, choices=sorted(PRODUCT_ROOT_PREFIX))
    parser.add_argument("--region", required=True, help="e.g. New_York, New_Mexico")
    args = parser.parse_args()

    cpus = get_slurm_cpus()
    print(f"Using {cpus} worker process(es)", flush=True)

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

    # Collect every (store, group, var_name, output_dir) task across every
    # store/group up front, then run all of them in one shared worker pool --
    # keeps CPUs busy across store/group boundaries instead of re-spinning a
    # pool per store for products with several (EDDEv2, Ouranos).
    tasks: list[tuple[Path, Optional[str], str, Path]] = []
    for store in stores:
        dirname = f"nan_times_{store.stem}" if parent_counts[store.parent] > 1 else "nan_times"
        output_dir = store.parent / dirname
        for group in store_groups(store):
            print_time_coverage(store, group)
            ds = xr.open_zarr(str(store), group=group, consolidated=False)
            if "time" not in ds.coords:
                continue
            output_dir.mkdir(parents=True, exist_ok=True)
            for var_name in ds.data_vars:
                if "time" in ds[var_name].dims:
                    tasks.append((store, group, var_name, output_dir))

    print(f"\nScanning {len(tasks)} (store, group, variable) task(s) across {cpus} worker(s)...\n", flush=True)
    results = Parallel(n_jobs=cpus, backend="loky", verbose=0)(
        delayed(scan_one_var)(store, group, var_name, output_dir)
        for store, group, var_name, output_dir in tasks
    )
    for r in results:
        print(r, flush=True)


if __name__ == "__main__":
    main()
