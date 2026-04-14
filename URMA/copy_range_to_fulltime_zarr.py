# %%
"""
Create a target Zarr with a full time index and copy a time-range from a source Zarr.

Example:
  python copy_range_to_fulltime_zarr.py \
    --src /path/to/URMA_NYS.zarr \
    --target /path/to/URMA_NYS_fulltime.zarr \
    --full-start 2010-01-01T00 \
    --full-end 2040-12-31T23 \
    --copy-start 2019-01-01T00 \
    --copy-end 2022-12-31T23 \
    --time-chunk 24 --y-chunk 256 --x-chunk 288
"""

import argparse
import os
import pandas as pd
import xarray as xr
import zarr

from URMA.process_and_write_to_zarr import ensure_initialized, project_dir


def is_interactive():
    import __main__ as main
    if not hasattr(main, "__file__"):
        return True
    return "ipykernel" in sys.argv[0]


def compute_time_slice(full_dates: pd.DatetimeIndex, start: str, end: str) -> slice:
    start = pd.to_datetime(start)
    end = pd.to_datetime(end)
    start_idx = full_dates.get_indexer([start])[0]
    end_idx = full_dates.get_indexer([end])[0]
    if start_idx < 0 or end_idx < 0:
        raise ValueError("copy-start/copy-end must exist on the full time index.")
    if end_idx < start_idx:
        raise ValueError("copy-end must be >= copy-start.")
    # end is inclusive; region slice end is exclusive
    return slice(start_idx, end_idx + 1)

# %%
if __name__ == "__main__":
    # %%
    parser = argparse.ArgumentParser(
        description="Create full-time Zarr and copy a time-range from a source Zarr."
    )
    parser.add_argument("--src", default="/network/rit/dgx/dgx_basulab/Projects/DFS/DATA/URMA_NYS/URMA_NYS.zarr",
                        help="Source Zarr (URMA) path")
    parser.add_argument("--target", default="/network/rit/dgx/dgx_basulab/Projects/DFS/DATA/URMA_NYS/URMA_NYS_rechunked.zarr",
                        help="Target Zarr path")
    parser.add_argument("--full-start", default="2010-01-01T00", help="Full time axis start (e.g., 2010-01-01T00)")
    parser.add_argument("--full-end", default="2040-12-31T23", help="Full time axis end (e.g., 2040-12-31T23)")
    parser.add_argument("--copy-start", default="2019-01-01T00", help="Copy range start (e.g., 2019-01-01T00)")
    parser.add_argument("--copy-end", default="2019-12-31T23", help="Copy range end (e.g., 2022-12-31T23)")
    parser.add_argument("--time-chunk", type=int, default=24, help="Chunk size for time dimension")
    parser.add_argument("--y-chunk", type=int, default=256, help="Chunk size for y dimension")
    parser.add_argument("--x-chunk", type=int, default=288, help="Chunk size for x dimension")
    parser.add_argument("--orog-path", default=f"{project_dir}/urma_nys_orography.nc", help="Orography path")
    parser.add_argument(
        "--vars",
        type=str,
        default=None,
        help="Comma-separated list of variables to copy (default: all variables in source)",
    )
    if is_interactive():
        args, _ = parser.parse_known_args()
    else:
        args = parser.parse_args()

    if not os.path.exists(args.src):
        raise FileNotFoundError(f"Source Zarr not found: {args.src}")

    full_dates = pd.date_range(args.full_start, args.full_end, freq="h")
    region = {"time": compute_time_slice(full_dates, args.copy_start, args.copy_end)}
    # %%
    ds0 = xr.open_zarr(args.src, consolidated=False)
    if args.vars:
        variables = [v.strip() for v in args.vars.split(",") if v.strip()]
    else:
        variables = list(ds0.data_vars)
    print(f"[init] Found variables: {variables}")
    # %%
    for var in variables:
        ensure_initialized(
            args.target,
            full_dates,
            var,
            orog_path=args.orog_path,
            time_chunk=args.time_chunk,
            x_chunk=args.x_chunk,
            y_chunk=args.y_chunk,
        )
    # %%
    ds = xr.open_zarr(args.src, consolidated=False)
    ds = ds[variables]
    ds = ds.drop_vars(["latitude", "longitude"], errors="ignore")
    ds = ds.sel(time=slice(args.copy_start, args.copy_end))
    ds = ds.chunk({"time": args.time_chunk, "y": args.y_chunk, "x": args.x_chunk})
    ds.to_zarr(args.target, mode="a", region=region, consolidated=False)
    print(f"[write] {args.copy_start}..{args.copy_end} -> {args.target} region={region}")

    zarr.consolidate_metadata(args.target)
    print(f"[done] Consolidated metadata at {args.target}")
# %%
