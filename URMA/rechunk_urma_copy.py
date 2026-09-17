#!/usr/bin/env python
"""Copy one variable/year of real data from the canonical URMA_NMS.zarr store
into a sibling store built with a different on-disk time-chunk size (see
process_and_write_to_zarr.py's --time-chunk/--store-suffix, and
jobsub_rechunk_urma_init.slurm which builds the sibling's skeleton).

Part of the 2026-09 6h->24h rechunk migration (URMA_NMS.zarr was built with an
unexamined hardcoded time_chunk=6; every other product in this repo uses 24).
Not a general-purpose tool -- reads from the canonical store, writes into the
sibling named by --store-suffix, one variable/year at a time so this can be
fanned out across many small, independent SLURM jobs (see run_rechunk_urma.sh).

Reuses the exact same data_utils/zarr_io.py write path production ingestion
uses (write_region + apply_var_attrs), so this is a pure copy: no regridding,
no unit conversion beyond apply_var_attrs' normal no-op-when-already-canonical
behavior, no new write logic invented.

Usage:
    python rechunk_urma_copy.py --region New_Mexico --variable t2m --year 2020 \
        --store-suffix _rechunk24h --time-chunk 24
"""
import argparse
import sys
from pathlib import Path

import pandas as pd
import xarray as xr
import zarr

BOOTSTRAP_ROOT = Path(__file__).resolve().parents[1]
if str(BOOTSTRAP_ROOT) not in sys.path:
    sys.path.insert(0, str(BOOTSTRAP_ROOT))

from repo_utils import load_region_grid, load_region_vars
from data_utils.zarr_io import apply_var_attrs, write_region


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--region", required=True, help="e.g. New_Mexico")
    parser.add_argument("--variable", required=True, help="URMA variable name, e.g. t2m")
    parser.add_argument("--year", type=int, required=True)
    parser.add_argument("--store-suffix", required=True, help="e.g. _rechunk24h -- must match the sibling built by jobsub_rechunk_urma_init.slurm")
    parser.add_argument("--time-chunk", type=int, default=24)
    parser.add_argument("--full-start-year", type=int, default=2010)
    parser.add_argument("--full-end-year", type=int, default=2040)
    args = parser.parse_args()

    region_grid_raw = load_region_grid(args.region, "URMA")
    region_vars = load_region_vars(args.region)
    if "inner" in region_grid_raw:
        region_grid = {"dims": region_grid_raw["dims"], **region_grid_raw["outer"]}
    else:
        region_grid = region_grid_raw
    ny = region_grid["n0"]
    nx = region_grid["n1"]
    data_root = region_vars["data_root"]
    region_tag = region_vars["region_tag"]

    old_store = f"{data_root}/URMA_{region_tag}/URMA_{region_tag}.zarr"
    new_store = f"{data_root}/URMA_{region_tag}/URMA_{region_tag}{args.store_suffix}.zarr"

    full_dates = pd.date_range(
        f"{args.full_start_year}-01-01T00", f"{args.full_end_year}-12-31T23", freq="h",
    )

    print(f"Opening old store: {old_store}")
    ds_old = xr.open_zarr(old_store, consolidated=False)
    if args.variable not in ds_old.data_vars:
        raise KeyError(f"{args.variable} not found in {old_store}")

    time_slice = slice(f"{args.year}-01-01T00", f"{args.year}-12-31T23")
    da = ds_old[[args.variable]].sel(time=time_slice)
    if da.time.size == 0:
        print(f"[skip] {args.variable} {args.year}: no timestamps in range, nothing to copy")
        return

    n_real = int((~da[args.variable].isnull()).any(dim=[d for d in da[args.variable].dims if d != "time"]).sum().compute())
    if n_real == 0:
        print(f"[skip] {args.variable} {args.year}: all-NaN in this range, nothing real to copy")
        return

    ds_slice = apply_var_attrs(da, args.variable)

    zarr_sync = zarr.ProcessSynchronizer(f"{new_store}.sync")
    print(f"Writing {args.variable} {args.year} ({da.time.size} hours, {n_real} with real data) -> {new_store}")
    write_region(
        ds_slice, new_store, full_dates,
        {"time": args.time_chunk, "y": ny, "x": nx},
        synchronizer=zarr_sync,
    )
    print(f"[write] {args.variable} {args.year}: wrote to {new_store}")


if __name__ == "__main__":
    main()
