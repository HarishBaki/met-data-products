#!/usr/bin/env python3
"""
Download Ouranos CRCM5-CMIP6 (NAM-12, any frequency) region-cropped data and
write directly to per-run Zarr stores under Ouranos_{region_tag} (--region,
default New_York), one (catalog row, variable, year) per job. The crop bbox
and output/raw-staging locations come from configs/regions/{region}.yaml via
download_ouranos.py's resolve_region_bbox()/resolve_region_output_root() --
see this script's resolve_region_raw_root()/resolve_region_orog_path().

Design:
- `--var init` pre-creates a NaN-filled Zarr store spanning the full
  sim_start_year..sim_end_year time axis (resolution depends on --frequency),
  with all canonical variables for that frequency. Run this once per catalog
  row before any per-variable jobs (cheap/no-op if the store and variable
  already exist).
- For "download" variables, fetch one CORDEX variable for one year via
  download_ouranos.py's THREDDS NCSS helpers, rename to the canonical (or, if
  no canonical alias exists, native CORDEX) name, convert units, and
  region-write that year into the store.
- For "derived" variables (si10, wdir10), read the already-written u10/v10 for
  that year back from the zarr store (no NCSS request) and compute them.
- Downloaded yearly NetCDF files are deleted after a successful write unless
  --keep-raw is given (disk-space management).
- Before attempting any download, a per-row availability check against the
  loaded --catalog file's vars_{frequency} column skips variables that don't
  actually exist for that row/frequency (varies per row, not just per
  frequency - e.g. ERA5 evaluation v1-r2 has only `pr` at 1hr). --catalog
  defaults to catalog_with_vars.csv, which already has these columns.

Variable mapping (CORDEX -> canonical), per frequency, see VAR_GROUPS_BY_FREQUENCY.
si10/wdir10 are derived from u10/v10 wherever both exist for a frequency (not
downloaded). tp/prsn (precipitation flux) are intentionally omitted at day/mon:
var_meta.yaml's only kg-m-2-s-1 conversion factor assumes a 1-hour accumulation
window and would silently under-convert at lower frequencies.
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
    FREQUENCIES,
    build_dest,
    build_url,
    download_one,
    load_catalog,
    load_region_vars,
    resolve_region_bbox,
    resolve_region_output_root,
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

DEFAULT_CATALOG = Path(__file__).parent / "catalog_with_vars.csv"

# --output-root/--raw-root are region-derived when not explicitly overridden --
# see resolve_region_raw_root() below and resolve_region_output_root() (imported)
# and main()'s output_root resolution. OROG_PATH/REGION are set from the resolved
# output_root in main(): get_template() reads them as module globals, which is
# safe here since this script has no parallelism/subprocess forking (one
# (row, var, year) per invocation).
OROG_PATH = None
REGION = None


def resolve_region_raw_root(region: str) -> Path:
    """Raw yearly-download staging root for a region. Region-scoped (unlike
    URMA/HRRR/etc.'s raw archives, which cover the full native grid regardless
    of region) because these files are already cropped to this region's bbox
    at download time (see build_url's bbox param, from download_ouranos.
    resolve_region_bbox) -- keeping a single shared, non-region-tagged
    directory across regions would be confusing even though build_dest's
    domain_tag already keeps filenames themselves collision-free."""
    region_tag = load_region_vars(region)["region_tag"]
    return Path(f"/network/rit/lab/basulab/RAW_DATA/Ouranos_{region_tag}/1hr")


def resolve_region_orog_path(region: str, output_root: Path) -> Path:
    """Path to the region's already-downloaded static orography file -- the
    spatial template init_store()/get_template() need. Produced by running
    download_fx.py --region {region} --vars orog against the ERA5/evaluation
    row first; filename convention (v1-r1_{region_tag_lower}_orography.nc4)
    matches build_fx_dest's domain_tag.lower() naming. Doesn't exist yet for
    a brand-new region (e.g. New Mexico) until that download is run once."""
    region_tag = load_region_vars(region)["region_tag"]
    return output_root / "ERA5/evaluation/r1i1p1" / f"v1-r1_{region_tag.lower()}_orography.nc4"

TIME_CHUNK_BY_FREQUENCY = {"1hr": 24, "3hr": 240, "day": 365, "mon": 120}

# Per-variable processing units: each is one job (--index, --var, --year, --frequency).
# "download" vars fetch one CORDEX variable from PAVICS NCSS, rename, and convert
# units. "derived" vars are computed from canonical vars already written to the
# zarr store (no NCSS request) - their jobs must run after their deps' jobs.
#
# Variable-naming policy: reuse canonical short names (t2m, sp, sh2, tp, rh2,
# u10, v10) where the CORDEX source exists at that frequency; fall back to the
# native CORDEX name otherwise (tasmax, tasmin, prsn, snw, ...). data_utils
# apply_var_attrs() already no-ops gracefully for names absent from
# var_meta.yaml (keeps source units, long_name=var_name) - no changes needed
# there for native names to pass through safely.
VAR_GROUPS_BY_FREQUENCY = {
    "1hr": {
        "t2m":    {"kind": "download", "source_var": "tas"},
        "sp":     {"kind": "download", "source_var": "ps"},
        "sh2":    {"kind": "download", "source_var": "huss"},
        "tp":     {"kind": "download", "source_var": "pr"},
        "rh2":    {"kind": "download", "source_var": "hurs"},
        "u10":    {"kind": "download", "source_var": "uas"},
        "v10":    {"kind": "download", "source_var": "vas"},
        "si10":   {"kind": "derived", "deps": ["u10", "v10"]},
        "wdir10": {"kind": "derived", "deps": ["u10", "v10"]},
    },
    "3hr": {
        # No row has any canonical-7 overlap at 3hr (verified: intersection
        # across all 27 rows' vars_3hr is {clwvi, hfls, mrro, prra, prsn, prw,
        # snw} - none of tas/ps/huss/pr/hurs/uas/vas). Native names only;
        # prsn omitted - see module docstring (flux-unit risk).
        "clwvi": {"kind": "download", "source_var": "clwvi"},
        "hfls":  {"kind": "download", "source_var": "hfls"},
        "mrro":  {"kind": "download", "source_var": "mrro"},
        "prw":   {"kind": "download", "source_var": "prw"},
        "snw":   {"kind": "download", "source_var": "snw"},
    },
    "day": {
        "t2m":    {"kind": "download", "source_var": "tas"},
        "tasmax": {"kind": "download", "source_var": "tasmax"},
        "tasmin": {"kind": "download", "source_var": "tasmin"},
        "snw":    {"kind": "download", "source_var": "snw"},
        # tp/prsn deliberately omitted - flux-unit conversion factor is 1hr-specific.
    },
    "mon": {
        "t2m":    {"kind": "download", "source_var": "tas"},
        "sp":     {"kind": "download", "source_var": "ps"},
        "sh2":    {"kind": "download", "source_var": "huss"},
        "rh2":    {"kind": "download", "source_var": "hurs"},
        "u10":    {"kind": "download", "source_var": "uas"},
        "v10":    {"kind": "download", "source_var": "vas"},
        "si10":   {"kind": "derived", "deps": ["u10", "v10"]},
        "wdir10": {"kind": "derived", "deps": ["u10", "v10"]},
        "tasmax": {"kind": "download", "source_var": "tasmax"},
        "tasmin": {"kind": "download", "source_var": "tasmin"},
        "snw":    {"kind": "download", "source_var": "snw"},
        # tp/prsn deliberately omitted - same reason as day.
    },
}
ALL_VARS_BY_FREQUENCY = {f: list(g) for f, g in VAR_GROUPS_BY_FREQUENCY.items()}
ALL_VARS_UNION = sorted({v for g in VAR_GROUPS_BY_FREQUENCY.values() for v in g})


def get_template() -> xr.Dataset:
    """Spatial template (rlat/rlon dims, 2D lat/lon coords) shared by all runs."""
    if not OROG_PATH.exists():
        raise FileNotFoundError(
            f"Orography template not found: {OROG_PATH} -- run download_fx.py --region {REGION} "
            f"--vars orog first (it writes this file as a side effect)."
        )
    ds = xr.open_dataset(OROG_PATH)
    promote = [v for v in ("lat", "lon") if v in ds.data_vars]
    if promote:
        ds = ds.set_coords(promote)
    return ds


def output_zarr_path(output_root: Path, row: dict, frequency: str, region_tag: str) -> str:
    return str(output_root / row["dest_subdir"] / f"{row['realization']}_{frequency}_{region_tag}.zarr")


# ---------------------------------------------------------------------------
# Per-frequency time axis
# ---------------------------------------------------------------------------

def _month_midpoints(start_year: int, end_year: int) -> pd.DatetimeIndex:
    """CF 'midpoint of the averaging period' convention: varies with month length."""
    starts = pd.date_range(f"{start_year}-01-01", f"{end_year}-12-01", freq="MS")
    nexts = starts + pd.DateOffset(months=1)
    return pd.DatetimeIndex([s + (n - s) / 2 for s, n in zip(starts, nexts)])


_FREQ_AXIS_BUILDERS = {
    "1hr": lambda s, e: pd.date_range(f"{s}-01-01",       f"{e}-12-31 23:00", freq="1h"),
    "3hr": lambda s, e: pd.date_range(f"{s}-01-01",       f"{e}-12-31 21:00", freq="3h"),
    "day": lambda s, e: pd.date_range(f"{s}-01-01 12:00", f"{e}-12-31 12:00", freq="1D"),
    "mon": lambda s, e: _month_midpoints(s, e),
}


def full_time_axis(row: dict, frequency: str) -> pd.DatetimeIndex:
    return _FREQ_AXIS_BUILDERS[frequency](row["sim_start_year"], row["sim_end_year"])


def year_times_for(year: int, frequency: str) -> tuple[pd.DatetimeIndex, pd.DatetimeIndex]:
    """(full-year times at this frequency, same minus Feb-29 for noleap-calendar checks)."""
    year_times = _FREQ_AXIS_BUILDERS[frequency](year, year)
    check_times = year_times[~((year_times.month == 2) & (year_times.day == 29))]
    return year_times, check_times


# ---------------------------------------------------------------------------
# Per-row variable availability (row's own vars_{frequency} column, already
# loaded as part of --catalog - see module docstring)
# ---------------------------------------------------------------------------

def is_var_available(row: dict, source_var: str, frequency: str) -> bool:
    cell = row.get(f"vars_{frequency}", "")
    return source_var in cell.split(";")


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
    frequency: str, bbox: dict, domain_tag: str,
) -> str:
    source_var = VAR_GROUPS_BY_FREQUENCY[frequency][var_name]["source_var"]
    _, check_times = year_times_for(year, frequency)
    if not has_missing_data(output_zarr, check_times, var_name, synchronizer=zarr_sync):
        return f"[skip] {row['dest_subdir']}/{row['realization']} {var_name} {year} already complete"

    if not is_var_available(row, source_var, frequency):
        return (
            f"[skip-unavailable] {row['dest_subdir']}/{row['realization']} {var_name} "
            f"({source_var}) {year}: not present in vars_{frequency} for this row"
        )

    url = build_url(
        row, [source_var], f"{year}-01-01T00:00:00Z", f"{year}-12-31T23:00:00Z", bbox,
        frequency=frequency,
    )
    dest = build_dest(raw_root, row, [source_var], str(year), domain_tag, frequency=frequency)
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
    chunks: dict, zarr_sync: zarr.ProcessSynchronizer, frequency: str,
) -> str:
    year_times, check_times = year_times_for(year, frequency)
    if not has_missing_data(output_zarr, check_times, var_name, synchronizer=zarr_sync):
        return f"[skip] {row['dest_subdir']}/{row['realization']} {var_name} {year} already complete"

    deps = VAR_GROUPS_BY_FREQUENCY[frequency][var_name]["deps"]
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


def init_store(
    row: dict, output_zarr: str, full_times: pd.DatetimeIndex, chunks: dict, frequency: str,
    region_tag: str,
) -> None:
    # No synchronizer here: this is a standalone job that must complete before any
    # per-variable jobs for this row start (see process_and_write_to_zarr.slurm /
    # run_all_process_and_write_to_zarr.sh's polling-based wait). Under NFS,
    # zarr.ProcessSynchronizer's per-chunk file locks make each ensure_store() call
    # take minutes for no benefit when there's no concurrent writer to coordinate
    # against. For a row whose store already has all variables, each call below is
    # a fast no-op (open + time-axis check).
    for var_name in ALL_VARS_BY_FREQUENCY[frequency]:
        t0 = time.monotonic()
        ensure_store(
            output_zarr, full_times, var_name, get_template, chunks,
            global_title=(
                f"Ouranos NAM-12 CRCM5 {frequency} {region_tag} - {row['source_id']} "
                f"{row['experiment_id']} {row['realization']}"
            ),
        )
        print(f"[ensure_store] {var_name} done in {time.monotonic() - t0:.1f}s", flush=True)


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    p.add_argument(
        "--catalog", default=str(DEFAULT_CATALOG),
        help="Path to catalog CSV (default: catalog_with_vars.csv next to this script, "
             "whose vars_1hr/3hr/day/mon columns drive the per-row availability check)",
    )
    p.add_argument("--index", type=int, required=True, help="Catalog row index")
    p.add_argument(
        "--var", required=True, choices=ALL_VARS_UNION + ["init"],
        help="Variable to process, or 'init' to pre-create the zarr store skeleton "
             "(valid choices depend on --frequency; see VAR_GROUPS_BY_FREQUENCY)",
    )
    p.add_argument(
        "--frequency", default="1hr", choices=FREQUENCIES,
        help="Time frequency to process (default: 1hr)",
    )
    p.add_argument("--year", type=int, default=None, help="Year to process (required unless --var init)")
    p.add_argument(
        "--region", type=str, required=True,
        help="Region config name under configs/regions/ (e.g. New_York, New_Mexico) -- "
             "supplies the download bbox (grid.Ouranos.bbox) and, when --output-root/"
             "--raw-root are not given explicitly, those locations too. REQUIRED, no "
             "default -- an implicit New_York default previously risked silently running "
             "against the wrong region's data if omitted."
    )
    p.add_argument(
        "--output-root", default=None,
        help="Root dir for output Zarr stores. Default: {data_root}/Ouranos_{region_tag}, from --region.",
    )
    p.add_argument(
        "--raw-root", default=None,
        help="Root dir for downloaded yearly NetCDF files. "
             "Default: /network/rit/lab/basulab/RAW_DATA/Ouranos_{region_tag}/1hr, from --region.",
    )
    p.add_argument("--keep-raw", action="store_true", help="Keep downloaded yearly NetCDF files after writing to zarr")
    return p.parse_args()


def main() -> None:
    args = parse_args()
    catalog = load_catalog(Path(args.catalog))
    matches = [r for r in catalog if r["index"] == args.index]
    if not matches:
        raise SystemExit(f"No catalog row with index {args.index}")
    row = matches[0]

    freq_vars = VAR_GROUPS_BY_FREQUENCY[args.frequency]
    if args.var != "init" and args.var not in freq_vars:
        raise SystemExit(
            f"--var {args.var!r} is not defined for --frequency {args.frequency!r}; "
            f"valid: {list(freq_vars)}"
        )

    output_root = Path(args.output_root) if args.output_root else resolve_region_output_root(args.region)
    raw_root = Path(args.raw_root) if args.raw_root else resolve_region_raw_root(args.region)
    region_tag = load_region_vars(args.region)["region_tag"]

    global OROG_PATH, REGION
    OROG_PATH = resolve_region_orog_path(args.region, output_root)
    REGION = args.region

    full_times = full_time_axis(row, args.frequency)
    output_zarr = output_zarr_path(output_root, row, args.frequency, region_tag)
    chunks = {"time": TIME_CHUNK_BY_FREQUENCY[args.frequency]}

    Path(output_zarr).parent.mkdir(parents=True, exist_ok=True)

    print(
        f"[row {row['index']}] {row['source_id']} {row['experiment_id']} "
        f"{row['realization']} -> {output_zarr} var={args.var} freq={args.frequency} year={args.year}",
        flush=True,
    )

    if args.var == "init":
        init_store(row, output_zarr, full_times, chunks, args.frequency, region_tag)
        return

    if args.year is None:
        raise SystemExit("--year is required unless --var init")

    zarr_sync = zarr.ProcessSynchronizer(f"{output_zarr}.sync")
    info = freq_vars[args.var]
    if info["kind"] == "download":
        bbox = resolve_region_bbox(args.region)
        result = process_download_var(
            row, args.var, args.year, output_zarr, full_times, chunks, raw_root, args.keep_raw,
            zarr_sync, args.frequency, bbox, region_tag,
        )
    else:
        result = process_derived_var(
            row, args.var, args.year, output_zarr, full_times, chunks, zarr_sync, args.frequency,
        )
    print(result, flush=True)


if __name__ == "__main__":
    main()
