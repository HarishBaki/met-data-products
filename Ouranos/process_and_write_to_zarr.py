#!/usr/bin/env python3
"""
Download Ouranos CRCM5-CMIP6 (NAM-12, 1hr) NYS-cropped data and write directly to
per-run Zarr stores under Ouranos_NYS, one (catalog row, variable, year) per job.

Design:
- `--var init` pre-creates a NaN-filled Zarr store spanning the full
  sim_start_year..sim_end_year hourly time axis, with all canonical variables.
  Run this once per catalog row before any per-variable jobs (cheap/no-op if the
  store and variable already exist).
- For "download" variables, fetch one CORDEX variable for one year via
  download_ouranos.py's THREDDS NCSS helpers, rename to the canonical name,
  convert units, and region-write that year into the store.
- For "derived" variables (si10, wdir10), read the already-written u10/v10 for
  that year back from the zarr store (no NCSS request) and compute them.
- Downloaded yearly NetCDF files are deleted after a successful write unless
  --keep-raw is given (disk-space management).

Variable mapping (CORDEX -> canonical):
  tas->t2m  ps->sp  uas->u10  vas->v10  huss->sh2  pr->tp  hurs->rh2
  si10, wdir10 are derived from u10/v10 (not downloaded).
"""

import argparse
import sys
import time
from pathlib import Path

import numpy as np
import pandas as pd
import xarray as xr
import zarr

from download_ouranos import (
    BBOX_NY,
    build_dest,
    build_url,
    download_one,
    load_catalog,
)

_BOOTSTRAP_ROOT = Path(__file__).resolve().parents[1]
if str(_BOOTSTRAP_ROOT) not in sys.path:
    sys.path.insert(0, str(_BOOTSTRAP_ROOT))

from data_utils.zarr_io import (  # noqa: E402
    apply_var_attrs,
    ensure_store,
    has_missing_data,
    open_zarr_safe,
    write_region,
)

DEFAULT_CATALOG = Path(__file__).parent / "catalog.csv"
RAW_ROOT_DEFAULT = Path("/network/rit/lab/basulab/RAW_DATA/Ouranos/1hr")
OUTPUT_ROOT_DEFAULT = Path("/network/rit/lab/basulab/Projects/DFS/DATA/Ouranos_NYS")
OROG_PATH = (
    OUTPUT_ROOT_DEFAULT
    / "NAM-12_ERA5_evaluation_r1i1p1_OURANOS_CRCM5_v1-r1_NYS_areacella+orog_static.nc4"
)

TIME_CHUNK = 24

# Per-variable processing units: each is one job (--index, --var, --year).
# "download" vars fetch one CORDEX variable from PAVICS NCSS, rename, and convert
# units. "derived" vars are computed from canonical vars already written to the
# zarr store (no NCSS request) - their jobs must run after their deps' jobs.
VAR_GROUPS = {
    "t2m":    {"kind": "download", "source_var": "tas"},
    "sp":     {"kind": "download", "source_var": "ps"},
    "sh2":    {"kind": "download", "source_var": "huss"},
    "tp":     {"kind": "download", "source_var": "pr"},
    "rh2":    {"kind": "download", "source_var": "hurs"},
    "u10":    {"kind": "download", "source_var": "uas"},
    "v10":    {"kind": "download", "source_var": "vas"},
    "si10":   {"kind": "derived", "deps": ["u10", "v10"]},
    "wdir10": {"kind": "derived", "deps": ["u10", "v10"]},
}
ALL_VARS = list(VAR_GROUPS)


def get_template() -> xr.Dataset:
    """Spatial template (rlat/rlon dims, 2D lat/lon coords) shared by all runs."""
    ds = xr.open_dataset(OROG_PATH)
    promote = [v for v in ("lat", "lon") if v in ds.data_vars]
    if promote:
        ds = ds.set_coords(promote)
    return ds


def output_zarr_path(output_root: Path, row: dict) -> str:
    return str(output_root / row["dest_subdir"] / f"{row['realization']}_NYS.zarr")


def full_time_axis(row: dict) -> pd.DatetimeIndex:
    return pd.date_range(
        f"{row['sim_start_year']}-01-01", f"{row['sim_end_year']}-12-31 23:00", freq="1h"
    )


def year_times_for(year: int) -> tuple[pd.DatetimeIndex, pd.DatetimeIndex]:
    """(full-year hourly times, same minus Feb-29 for noleap-calendar resumability checks)."""
    year_times = pd.date_range(f"{year}-01-01", f"{year}-12-31 23:00", freq="1h")
    check_times = year_times[~((year_times.month == 2) & (year_times.day == 29))]
    return year_times, check_times


def _finalize_and_write(
    ds: xr.Dataset, var_name: str, output_zarr: str, full_times: pd.DatetimeIndex,
    chunks: dict, zarr_sync: zarr.ProcessSynchronizer,
) -> None:
    """ensure_store (scoped to this one var) + clean attrs/encoding + write_region."""
    ensure_store(output_zarr, full_times, var_name, get_template, chunks, synchronizer=zarr_sync)

    # Drop inherited encoding/attrs (e.g. _FillValue from the source NetCDF or
    # propagated through arithmetic ops): apply_var_attrs sets canonical
    # long_name/units/_FillValue/missing_value, which conflicts with a leftover
    # encoding["_FillValue"] at to_zarr() time otherwise.
    ds[var_name].encoding = {}
    ds[var_name].attrs.pop("_FillValue", None)
    ds[var_name].attrs.pop("missing_value", None)

    # rlat/rlon are dimension coordinates but were already written once by
    # ensure_store; to_zarr(region=...) requires every coord to share the
    # "time" region dim, so drop everything else here.
    ds_write = ds[[var_name]]
    ds_write = ds_write.drop_vars([c for c in ds_write.coords if c != "time"])
    write_region(ds_write, output_zarr, full_times, chunks, synchronizer=zarr_sync)


def process_download_var(
    row: dict, var_name: str, year: int, output_zarr: str, full_times: pd.DatetimeIndex,
    chunks: dict, raw_root: Path, keep_raw: bool, zarr_sync: zarr.ProcessSynchronizer,
) -> str:
    source_var = VAR_GROUPS[var_name]["source_var"]
    _, check_times = year_times_for(year)
    if not has_missing_data(output_zarr, check_times, var_name, synchronizer=zarr_sync):
        return f"[skip] {row['dest_subdir']}/{row['realization']} {var_name} {year} already complete"

    url = build_url(row, [source_var], f"{year}-01-01T00:00:00Z", f"{year}-12-31T23:00:00Z", BBOX_NY)
    dest = build_dest(raw_root, row, [source_var], str(year), full_domain=False)
    result = download_one(url, dest)
    if result.startswith("FAIL"):
        return f"[fail] {dest.name}: {result}"

    ds = xr.open_dataset(dest)
    if not isinstance(ds.indexes["time"], pd.DatetimeIndex):
        # CMIP6-driven rows (e.g. CanESM5, NorESM2-MM) use a 365_day/noleap calendar;
        # convert to standard so write_region's pd.DatetimeIndex/searchsorted logic and
        # the zarr store's datetime64 time axis work uniformly across all rows.
        # missing=np.nan fills Feb 29 (absent from noleap years) with NaN.
        ds = ds.convert_calendar("standard", align_on="date", missing=np.nan)
    ds = ds.rename({source_var: var_name})
    ds = apply_var_attrs(ds, var_name)

    _finalize_and_write(ds, var_name, output_zarr, full_times, chunks, zarr_sync)

    if not keep_raw:
        dest.unlink(missing_ok=True)

    return f"[write] {row['dest_subdir']}/{row['realization']} {var_name} {year} -> {output_zarr}"


def process_derived_var(
    row: dict, var_name: str, year: int, output_zarr: str, full_times: pd.DatetimeIndex,
    chunks: dict, zarr_sync: zarr.ProcessSynchronizer,
) -> str:
    year_times, check_times = year_times_for(year)
    if not has_missing_data(output_zarr, check_times, var_name, synchronizer=zarr_sync):
        return f"[skip] {row['dest_subdir']}/{row['realization']} {var_name} {year} already complete"

    deps = VAR_GROUPS[var_name]["deps"]
    for dep in deps:
        if has_missing_data(output_zarr, check_times, dep, synchronizer=zarr_sync):
            return (
                f"[wait] {row['dest_subdir']}/{row['realization']} {var_name} {year}: "
                f"{dep} not yet complete, run that job first"
            )

    src = open_zarr_safe(output_zarr, synchronizer=zarr_sync)[deps].sel(time=year_times)
    u10, v10 = src["u10"], src["v10"]
    if var_name == "si10":
        out = np.sqrt(u10 ** 2 + v10 ** 2).astype(np.float32)
    else:  # wdir10
        out = (
            ((270 - np.rad2deg(np.arctan2(v10, u10))) % 360)
            .where((u10 != 0) | (v10 != 0), other=0)
            .astype(np.float32)
        )
    # Start from a clean slate: arithmetic on u10/v10 may carry over their attrs
    # (e.g. long_name="10 m eastward wind"), which apply_var_attrs would otherwise
    # have to overwrite piecemeal.
    out.attrs = {}
    ds = out.to_dataset(name=var_name)
    ds = apply_var_attrs(ds, var_name)

    _finalize_and_write(ds, var_name, output_zarr, full_times, chunks, zarr_sync)

    return f"[write] {row['dest_subdir']}/{row['realization']} {var_name} {year} -> {output_zarr}"


def init_store(row: dict, output_zarr: str, full_times: pd.DatetimeIndex, chunks: dict) -> None:
    # No synchronizer here: this is a standalone job that must complete before any
    # per-variable jobs for this row start (see process_and_write_to_zarr.slurm /
    # run_all_process_and_write_to_zarr.sh's --dependency=afterok). Under NFS,
    # zarr.ProcessSynchronizer's per-chunk file locks make each ensure_store() call
    # take minutes for no benefit when there's no concurrent writer to coordinate
    # against. For a row whose store already has all variables, each call below is
    # a fast no-op (open + time-axis check).
    for var_name in ALL_VARS:
        t0 = time.monotonic()
        ensure_store(
            output_zarr, full_times, var_name, get_template, chunks,
            global_title=(
                f"Ouranos NAM-12 CRCM5 1hr NYS - {row['source_id']} "
                f"{row['experiment_id']} {row['realization']}"
            ),
        )
        print(f"[ensure_store] {var_name} done in {time.monotonic() - t0:.1f}s", flush=True)


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    p.add_argument("--catalog", default=str(DEFAULT_CATALOG), help="Path to catalog CSV")
    p.add_argument("--index", type=int, required=True, help="Catalog row index")
    p.add_argument(
        "--var", required=True, choices=ALL_VARS + ["init"],
        help="Variable to process, or 'init' to pre-create the zarr store skeleton",
    )
    p.add_argument("--year", type=int, default=None, help="Year to process (required unless --var init)")
    p.add_argument("--output-root", default=str(OUTPUT_ROOT_DEFAULT), help="Root dir for output Zarr stores")
    p.add_argument("--raw-root", default=str(RAW_ROOT_DEFAULT), help="Root dir for downloaded yearly NetCDF files")
    p.add_argument("--keep-raw", action="store_true", help="Keep downloaded yearly NetCDF files after writing to zarr")
    return p.parse_args()


def main() -> None:
    args = parse_args()
    catalog = load_catalog(Path(args.catalog))
    matches = [r for r in catalog if r["index"] == args.index]
    if not matches:
        raise SystemExit(f"No catalog row with index {args.index}")
    row = matches[0]

    output_root = Path(args.output_root)
    raw_root = Path(args.raw_root)
    full_times = full_time_axis(row)
    output_zarr = output_zarr_path(output_root, row)
    chunks = {"time": TIME_CHUNK}

    Path(output_zarr).parent.mkdir(parents=True, exist_ok=True)

    print(
        f"[row {row['index']}] {row['source_id']} {row['experiment_id']} "
        f"{row['realization']} -> {output_zarr} var={args.var} year={args.year}",
        flush=True,
    )

    if args.var == "init":
        init_store(row, output_zarr, full_times, chunks)
        return

    if args.year is None:
        raise SystemExit("--year is required unless --var init")

    zarr_sync = zarr.ProcessSynchronizer(f"{output_zarr}.sync")
    info = VAR_GROUPS[args.var]
    if info["kind"] == "download":
        result = process_download_var(
            row, args.var, args.year, output_zarr, full_times, chunks, raw_root, args.keep_raw, zarr_sync,
        )
    else:
        result = process_derived_var(row, args.var, args.year, output_zarr, full_times, chunks, zarr_sync)
    print(result, flush=True)


if __name__ == "__main__":
    main()
