#!/usr/bin/env python3
"""
Compare New Mexico's nan_times audit (compute_nan_times.py) against New
York's to separate gaps inherited from the shared source archive (also
missing in NY at the exact same calendar timestamp -- e.g. ICON's tp
month-boundary gap, the shared 2020-03-31 outage) from gaps that are
New-Mexico-specific and therefore real candidates for resubmission.

Only NM gaps inside New Mexico's actually-submitted range per product
(SUBMITTED_RANGES below, taken from each product's own run_all_*.sh) are
checked -- gaps outside that range are untouched store skeleton, not a
processing issue (see compute_nan_times.py's docstring on this).

Requires both regions' nan_times already computed (run_all_compute_nan_times.sh
for both --region New_Mexico and --region New_York first).

Usage:
    python compare_nan_times.py                       # all products
    python compare_nan_times.py --product URMA
"""
from __future__ import annotations

import argparse
from collections import Counter
from pathlib import Path
from typing import Optional

import pandas as pd
import xarray as xr

from compute_nan_times import PRODUCT_ROOT_PREFIX, discover_stores, store_groups
from repo_utils import load_region_vars

NM_REGION = "New_Mexico"
NY_REGION = "New_York"

# (start, end) inclusive, keyed by store stem ("*" = applies to every store
# for that product). Multiple entries needed only for EDDEv2, whose 3 run
# types have different ranges. Source: each product's own run_all_*.sh as of
# this session -- update here if those change.
SUBMITTED_RANGES: dict[str, dict[str, tuple[str, str]]] = {
    "URMA": {"*": ("2014-01-01", "2025-12-31")},
    "HRRR": {"*": ("2018-01-01", "2025-12-31")},
    "EDDEv2": {
        "Historical": ("1985-01-01", "2014-12-31"),
        "SSP2-4.5": ("2025-01-01", "2099-12-31"),
        "SSP3-7.0": ("2025-01-01", "2099-12-31"),
    },
    "ERA5": {"*": ("2018-01-01", "2025-12-31")},
    "ICON-DREAM-Global": {"*": ("2018-01-01", "2025-12-31")},
    "Ouranos": {"*": ("2018-01-01", "2024-12-31")},
}


def submitted_range(product: str, store_stem: str) -> Optional[tuple[str, str]]:
    ranges = SUBMITTED_RANGES.get(product, {})
    return ranges.get(store_stem, ranges.get("*"))


def nan_times_dir_for(store: Path, all_stores_for_product: list[Path]) -> Path:
    parent_counts = Counter(s.parent for s in all_stores_for_product)
    dirname = f"nan_times_{store.stem}" if parent_counts[store.parent] > 1 else "nan_times"
    return store.parent / dirname


def load_gap_times(nan_times_dir: Path, var_name: str, group: Optional[str]) -> pd.DatetimeIndex:
    name = f"nan_times_{var_name}.nc" if group is None else f"nan_times_{group}_{var_name}.nc"
    path = nan_times_dir / name
    if not path.exists():
        return pd.DatetimeIndex([])
    da = xr.open_dataarray(str(path))
    return pd.DatetimeIndex(da.values)


def product_root(product: str, region: str) -> Path:
    region_vars = load_region_vars(region)
    return Path(region_vars["data_root"]) / PRODUCT_ROOT_PREFIX[product].format(
        region_tag=region_vars["region_tag"]
    )


def normalized_rel(store: Path, root: Path, region_tag: str) -> str:
    """Store path relative to its product root, with the region tag blanked
    out -- some products bake the region tag into the store's filename
    itself, not just its containing directory (e.g. ERA5-ARCO's
    "ERA5_analysis_ARCO_NMS.zarr" vs New York's "..._NYS.zarr"), so a literal
    relative-path match would miss the pairing entirely. Also naturally
    excludes NY-only legacy stores that don't correspond to any current NM
    pipeline (e.g. ERA5's older "ERA5_analysis_DestinE_NYS.zarr" normalizes
    to a different string than "ERA5_analysis_ARCO_*.zarr" and simply never
    matches, with no special-casing needed)."""
    return str(store.relative_to(root)).replace(region_tag, "{REGION_TAG}")


def compare_product(product: str) -> None:
    print(f"\n{'=' * 70}\n{product}\n{'=' * 70}", flush=True)
    try:
        nm_stores = discover_stores(product, NM_REGION)
    except FileNotFoundError as e:
        print(f"  [skip] {e}")
        return
    nm_root = product_root(product, NM_REGION)
    nm_region_tag = load_region_vars(NM_REGION)["region_tag"]

    try:
        ny_stores = discover_stores(product, NY_REGION)
        ny_root = product_root(product, NY_REGION)
        ny_region_tag = load_region_vars(NY_REGION)["region_tag"]
        ny_by_norm = {normalized_rel(s, ny_root, ny_region_tag): s for s in ny_stores}
    except FileNotFoundError:
        ny_stores, ny_by_norm = [], {}

    for nm_store in nm_stores:
        rel = nm_store.relative_to(nm_root)
        ny_store = ny_by_norm.get(normalized_rel(nm_store, nm_root, nm_region_tag))
        nm_nan_dir = nan_times_dir_for(nm_store, nm_stores)
        ny_nan_dir = nan_times_dir_for(ny_store, ny_stores) if ny_store else None

        store_range = submitted_range(product, nm_store.stem)
        if store_range is None:
            print(f"  [warn] no known submitted range configured for store {nm_store.stem!r} -- skipping")
            continue
        range_start, range_end = pd.Timestamp(store_range[0]), pd.Timestamp(store_range[1])

        for group in store_groups(nm_store):
            ds = xr.open_zarr(str(nm_store), group=group, consolidated=False)
            if "time" not in ds.coords:
                continue
            label_prefix = f"{rel}" + (f"[{group}]" if group else "")
            for var_name in ds.data_vars:
                if "time" not in ds[var_name].dims:
                    continue
                nm_gaps = load_gap_times(nm_nan_dir, var_name, group)
                nm_gaps_in_range = nm_gaps[(nm_gaps >= range_start) & (nm_gaps <= range_end)]
                if len(nm_gaps_in_range) == 0:
                    continue

                if ny_nan_dir is None:
                    print(f"  {label_prefix}/{var_name}: {len(nm_gaps_in_range)} gap(s) in submitted "
                          f"range -- NO NY BASELINE (no matching store found for New York)")
                    continue

                ny_gaps = load_gap_times(ny_nan_dir, var_name, group)
                unexplained = nm_gaps_in_range[~nm_gaps_in_range.isin(ny_gaps)]
                if len(unexplained):
                    print(f"  [FLAG] {label_prefix}/{var_name}: {len(unexplained)}/{len(nm_gaps_in_range)} "
                          f"gap(s) NOT present in NY at the same timestamps -- resubmission candidate")
                    by_year = pd.Series(1, index=unexplained).resample("YS").sum()
                    for year, count in by_year[by_year > 0].items():
                        print(f"      {year.year}: {int(count)}")
                else:
                    print(f"  [ok] {label_prefix}/{var_name}: {len(nm_gaps_in_range)} gap(s), all also "
                          f"present in NY at the same timestamps (inherited from source archive, not a bug)")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--product", choices=sorted(PRODUCT_ROOT_PREFIX))
    args = parser.parse_args()
    for p in [args.product] if args.product else sorted(PRODUCT_ROOT_PREFIX):
        compare_product(p)


if __name__ == "__main__":
    main()
