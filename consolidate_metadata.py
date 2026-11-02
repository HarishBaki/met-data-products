#!/usr/bin/env python3
"""
Consolidate zarr metadata for a product's raw store(s) in a region.

Run once, after ALL of that product's per-(var[,year]) write jobs for the
region have finished -- not from every write job (that repeats the same
expensive full-store scan once per job for no reason; see the run_all_*.sh
scripts, which gate a single call to this script on a --dependency covering
every job they submitted, instead of passing --consolidate-metadata to each
one directly the way HRRR/ERA5 used to).

Reuses compute_nan_times.py's discover_stores() rather than duplicating each
product's store-layout logic (EDDEv2's per-run-type stores, Ouranos's per
catalog-row stores, ERA5-ARCO's zarr groups, etc.) -- consolidating the
top-level store path handles every nested group within it in one call, so no
group-level iteration is needed here.

Usage:
    python consolidate_metadata.py --product ICON-DREAM-Global --region New_Mexico
"""
from __future__ import annotations

import argparse
import time

import zarr

from compute_nan_times import PRODUCT_ROOT_PREFIX, discover_stores


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--product", required=True, choices=sorted(PRODUCT_ROOT_PREFIX))
    parser.add_argument("--region", required=True, help="e.g. New_York, New_Mexico")
    parser.add_argument(
        "--store-suffix", default="",
        help="If set, consolidate only stores whose basename ends with this suffix "
             "(e.g. _outer for URMA_NYS_outer.zarr).",
    )
    args = parser.parse_args()

    stores = discover_stores(args.product, args.region)
    if args.store_suffix:
        stores = [store for store in stores if store.stem.endswith(args.store_suffix)]
        if not stores:
            raise FileNotFoundError(
                f"No {args.product} store ending in {args.store_suffix!r} found for {args.region}"
            )
    print(f"Found {len(stores)} store(s) for product={args.product} region={args.region}", flush=True)
    for store in stores:
        print(f"[consolidate] starting {store}", flush=True)
        t0 = time.time()
        zarr.consolidate_metadata(str(store))
        print(f"[consolidate] done in {time.time() - t0:.1f}s -> {store}/.zmetadata", flush=True)


if __name__ == "__main__":
    main()
