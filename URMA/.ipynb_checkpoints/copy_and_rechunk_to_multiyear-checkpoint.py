# %%
"""
Copy existing per-year URMA Zarr stores into a single multi-year store, rechunking
to a uniform layout. Uses region writes to avoid reprocessing GRIB.

Default copy range: 2014–2025
Default target time span: 2010–2040
Default chunks: time=24, y=120, x=90
"""

import argparse
import os
import pandas as pd
import xarray as xr
import zarr
import sys

repo_root = "URMA_downloaders"
if repo_root not in sys.path:
    sys.path.append(repo_root)

from process_and_write_to_zarr import ensure_initialized, orog_path


BASE_SRC = "/network/rit/home/hb533188/basulab/Projects/DFS/DATA/URMA_NYS"
DEFAULT_TARGET = os.path.join(BASE_SRC, "URMA_NYS.zarr")


def year_path(year: int, base_dir: str) -> str:
    return os.path.join(base_dir, f"{year}.zarr")


def compute_time_index(full_dates: pd.DatetimeIndex, year: int) -> slice:
    year_dates = pd.date_range(f"{year}-01-01T00", f"{year}-12-31T23", freq="h")
    start = full_dates.get_indexer([year_dates[0]])[0]
    end = start + len(year_dates)
    return slice(start, end)


def copy_year(
    year: int,
    full_dates: pd.DatetimeIndex,
    target_store: str,
    src_dir: str,
    time_chunk: int,
    y_chunk: int,
    x_chunk: int,
):
    src = year_path(year, src_dir)
    if not os.path.exists(src):
        raise FileNotFoundError(f"Missing source store for {year}: {src}")

    ds = xr.open_zarr(src, consolidated=False)
    ds = ds.chunk({"time": time_chunk, "y": y_chunk, "x": x_chunk})

    region = {"time": compute_time_index(full_dates, year)}
    ds.to_zarr(target_store, mode="a", region=region, consolidated=False)
    print(f"[write] {year} -> {target_store} region={region}")    

def is_interactive():
    import __main__ as main
    return not hasattr(main, '__file__') or 'ipykernel' in sys.argv[0]


# %%
if __name__ == "__main__":
    # %%
    parser = argparse.ArgumentParser(
        description="Copy per-year URMA Zarrs into a single multi-year store with new chunks."
    )
    parser.add_argument("--copy-start-year", type=int, default=2014, help="First year to copy (inclusive)")
    parser.add_argument("--copy-end-year", type=int, default=2025, help="Last year to copy (inclusive)")
    parser.add_argument("--full-start-year", type=int, default=2010, help="Start year for the target time axis (inclusive)")
    parser.add_argument("--full-end-year", type=int, default=2040, help="End year for the target time axis (inclusive)")
    parser.add_argument("--src-dir", type=str, default=BASE_SRC, help="Directory of <year>.zarr stores")
    parser.add_argument("--target", type=str, default=DEFAULT_TARGET, help="Output multi-year Zarr path")
    parser.add_argument("--time-chunk", type=int, default=6, help="Chunk size for time dimension")
    parser.add_argument("--y-chunk", type=int, default=256, help="Chunk size for y dimension")
    parser.add_argument("--x-chunk", type=int, default=288, help="Chunk size for x dimension")

    # Handle Jupyter/IPython
    if is_interactive():
        args, unknown = parser.parse_known_args()
    else:
        args = parser.parse_args()

    # %%
    years_to_copy = list(range(args.copy_start_year, args.copy_end_year + 1))
    full_dates = pd.date_range(f"{args.full_start_year}-01-01T00", f"{args.full_end_year}-12-31T23", freq="h")

    # Discover variables from the first available year
    first_existing = None
    for y in years_to_copy:
        p = year_path(y, args.src_dir)
        if os.path.exists(p):
            first_existing = p
            break
    if first_existing is None:
        raise FileNotFoundError("No source Zarr stores found in the given range.")

    ds0 = xr.open_zarr(first_existing, consolidated=False)
    variables = list(ds0.data_vars)
    print(f"[init] Found variables: {variables}")

    # %%
    # Initialize target store and ensure all variables exist with the full time axis
    for var in variables:
        ensure_initialized(
            args.target,
            full_dates,
            var,
            orog_path=orog_path,
            time_chunk=args.time_chunk,
            x_chunk=args.x_chunk,
            y_chunk=args.y_chunk,
        )

    # %%
    # Copy each year with rechunking
    for year in years_to_copy:
        copy_year(
            year,
            full_dates,
            args.target,
            args.src_dir,
            args.time_chunk,
            args.y_chunk,
            args.x_chunk,
        )

    # Consolidate metadata for faster reads
    zarr.consolidate_metadata(args.target)
    print(f"[done] Consolidated metadata at {args.target}")
