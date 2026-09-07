#!/usr/bin/env python3
"""
Discover, live from the EDDEv2 S3 bucket, which variables actually exist for
each (frequency, run_type, experiment) - one row per combination, columns
ordered frequency, run_type, experiment, variables, mirroring the bucket's
own path hierarchy (s3://.../EDDE_V2/{frequency}/{run_type}/{experiment}/...).
The variables column is a ';'-separated, alphabetically sorted list, same
value style as Ouranos's catalog_with_vars.csv vars_* columns, just kept as
one column per row here rather than one column per frequency, since
frequency is itself a row-level field in this layout.

Unlike Ouranos's discover_vars.py (which opens each remote dataset over
OPeNDAP to inspect its variable list, since Ouranos's THREDDS aggregations
don't expose that any other way), EDDEv2 is plain S3 with self-describing
filenames: {var}.{experiment}.{driver}.EDDE-WRF.{freq}hr.NA12.{year}-{month}.raw.nc
So discovery here is just `s5cmd ls` (recursive, metadata-only, no file opens)
plus filename parsing - no OPeNDAP, no retries/backoff needed, cheap enough to
rerun whenever you want a fresh picture (e.g. after a new run_type like
WRF-Nor lands, or a new variable shows up).

run_type (WRF-MPI, WRF-Nor, ...) and experiment (Historical, SSP2-4.5,
SSP3-7.0, ...) are discovered dynamically per frequency, never hardcoded, so
this script does not need to be edited when EDDEv2 grows a new run_type or
experiment.

static/ (fixed grid-info files) has no experiment axis - the same handful of
grid files apply to every experiment of a given run_type - so its rows carry
frequency=static and an empty experiment field.

Output: catalog_with_vars.csv, one row per (frequency, run_type, experiment).

Example:
    python discover_vars.py
    python discover_vars.py --frequencies 6hourly,hourly --dry-run
"""

import argparse
import re
import subprocess
from pathlib import Path

import pandas as pd

BUCKET_PREFIX = "s3://epa-edde-v2/EDDE_V2"
DEFAULT_OUTPUT = Path(__file__).parent / "catalog_with_vars.csv"

# All top-level frequency folders that follow the standard
# {run_type}/{experiment}/{year_range}/{year}/{file} layout.
FREQUENCIES = ["hourly", "6hourly", "daily", "monthly", "hifr"]
VAR_COLUMNS = [f"vars_{freq}" for freq in FREQUENCIES]

# {var}.{experiment}.{driver}.EDDE-WRF.{freqtag}.NA12.{year}-{month}.raw.nc
# e.g. psl.SSP2-4.5.mpi.EDDE-WRF.6hr.NA12.2025-01.raw.nc
#      psl.Historical.nor.EDDE-WRF.6hr.NA12.1990-01.raw.nc
FILENAME_RE = re.compile(
    r"^(?P<var>[A-Za-z0-9]+)\."
    r"(?P<experiment>[A-Za-z0-9.\-]+)\."
    r"(?P<driver>[A-Za-z0-9]+)\."
    r"EDDE-WRF\."
    r"(?P<freqtag>[A-Za-z0-9]+)\."
    r"NA12\."
    r"(?P<year>\d{4})-(?P<month>\d{2})\."
    r"raw\.nc$"
)

# static/ files: {var}.hist.{DRIVER}.EDDE-WRF.fixed.NA12.raw.nc
STATIC_FILENAME_RE = re.compile(
    r"^(?P<var>[A-Za-z0-9]+)\.hist\.(?P<driver>[A-Za-z0-9]+)\.EDDE-WRF\.fixed\.NA12\.raw\.nc$"
)


# ---------------------------------------------------------------------------
# S3 listing (metadata only - no file opens, no downloads)
# ---------------------------------------------------------------------------

def list_subdirs(prefix: str) -> list[str]:
    """Names of immediate subdirectories under prefix (trailing '/' stripped)."""
    proc = subprocess.run(
        ["s5cmd", "--no-sign-request", "ls", prefix.rstrip("/") + "/"],
        capture_output=True, text=True,
    )
    if proc.returncode != 0:
        return []
    dirs = []
    for line in proc.stdout.splitlines():
        if line.strip().startswith("DIR"):
            name = line.strip().rsplit(None, 1)[-1]
            dirs.append(name.rstrip("/"))
    return dirs


def s5cmd_ls(prefix: str) -> list[str]:
    """Non-recursive `ls` of a directory's contents (bare filenames, no dirs)."""
    proc = subprocess.run(
        ["s5cmd", "--no-sign-request", "ls", prefix.rstrip("/") + "/"],
        capture_output=True, text=True,
    )
    if proc.returncode != 0:
        return []
    names = []
    for line in proc.stdout.splitlines():
        line = line.strip()
        if not line or line.startswith("DIR"):
            continue
        names.append(line.rsplit(None, 1)[-1])
    return names


def s5cmd_ls_recursive(prefix: str) -> list[str]:
    """Recursively list all keys under prefix (relative paths, no bucket/prefix)."""
    pattern = prefix.rstrip("/") + "/**"
    proc = subprocess.run(
        ["s5cmd", "--no-sign-request", "ls", pattern],
        capture_output=True, text=True,
    )
    if proc.returncode != 0:
        if "no object found" in (proc.stderr + proc.stdout).lower():
            return []
        raise RuntimeError(f"s5cmd ls (recursive) failed for {pattern}: {proc.stderr.strip()}")
    keys = []
    for line in proc.stdout.splitlines():
        line = line.strip()
        if not line or line.startswith("DIR"):
            continue
        # size-prefixed lines: "<date> <time> <size> <key>"
        parts = line.split(None, 3)
        key = parts[-1] if len(parts) >= 4 else line
        keys.append(key)
    return keys


# ---------------------------------------------------------------------------
# Discovery
# ---------------------------------------------------------------------------

def discover_frequency(freq: str, run_types_filter: list[str] | None) -> dict[tuple[str, str], set[str]]:
    """Returns {(run_type, experiment): {variable names}} for this frequency."""
    freq_prefix = f"{BUCKET_PREFIX}/{freq}"
    run_types = list_subdirs(freq_prefix)
    if run_types_filter:
        run_types = [r for r in run_types if r in run_types_filter]

    result: dict[tuple[str, str], set[str]] = {}
    for run_type in run_types:
        run_prefix = f"{freq_prefix}/{run_type}"
        print(f"  Listing {run_prefix} ...", flush=True)
        keys = s5cmd_ls_recursive(run_prefix)
        print(f"    {len(keys)} file(s)", flush=True)

        unparsed = 0
        for key in keys:
            fname = key.rsplit("/", 1)[-1]
            m = FILENAME_RE.match(fname)
            if not m:
                unparsed += 1
                continue
            experiment = m.group("experiment")
            result.setdefault((run_type, experiment), set()).add(m.group("var"))

        if unparsed:
            print(f"    WARNING: {unparsed} filename(s) under {run_prefix} did not match "
                  f"the expected pattern (skipped) - inspect if this count looks high", flush=True)

    return result


def discover_static(run_types_filter: list[str] | None) -> dict[str, set[str]]:
    """Returns {run_type: {variable names}} for static/ (no experiment axis)."""
    static_prefix = f"{BUCKET_PREFIX}/static"
    run_types = list_subdirs(static_prefix)
    if run_types_filter:
        run_types = [r for r in run_types if r in run_types_filter]

    print(f"=== static ===", flush=True)
    result: dict[str, set[str]] = {}
    for run_type in run_types:
        names = s5cmd_ls(f"{static_prefix}/{run_type}")
        vars_ = {STATIC_FILENAME_RE.match(n).group("var") for n in names if STATIC_FILENAME_RE.match(n)}
        result[run_type] = vars_
        print(f"  {run_type}: {len(vars_)} var(s)", flush=True)
    return result


def cell_value(vars_: set[str] | None) -> str:
    if not vars_:
        return ""
    return ";".join(sorted(vars_))


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def parse_args():
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--frequencies", default=",".join(FREQUENCIES),
                   help=f"Comma-separated subset of {FREQUENCIES} (default: all)")
    p.add_argument("--run-types", default=None,
                   help="Comma-separated run_type filter, e.g. WRF-MPI,WRF-Nor (default: whatever exists)")
    p.add_argument("--output", default=str(DEFAULT_OUTPUT), help="Output CSV path")
    p.add_argument("--dry-run", action="store_true",
                   help="Only print which (frequency, run_type) prefixes would be listed, no S3 calls")
    return p.parse_args()


def main():
    args = parse_args()
    frequencies = [f.strip() for f in args.frequencies.split(",") if f.strip()]
    unknown = set(frequencies) - set(FREQUENCIES)
    if unknown:
        raise SystemExit(f"Unknown frequency/frequencies: {sorted(unknown)} (valid: {FREQUENCIES})")
    run_types_filter = [r.strip() for r in args.run_types.split(",")] if args.run_types else None

    if args.dry_run:
        for freq in frequencies:
            print(f"{BUCKET_PREFIX}/{freq}/{{run_type}}/**")
        return

    rows = []
    for freq in frequencies:
        print(f"=== {freq} ===", flush=True)
        freq_result = discover_frequency(freq, run_types_filter)  # {(run_type, experiment): {vars}}
        for (run_type, experiment), vars_ in sorted(freq_result.items()):
            rows.append({
                "frequency": freq,
                "run_type": run_type,
                "experiment": experiment,
                "variables": cell_value(vars_),
            })

    static_vars = discover_static(run_types_filter)  # {run_type: {vars}}
    for run_type, vars_ in sorted(static_vars.items()):
        rows.append({
            "frequency": "static",
            "run_type": run_type,
            "experiment": "",
            "variables": cell_value(vars_),
        })

    catalog = pd.DataFrame(rows, columns=["frequency", "run_type", "experiment", "variables"])
    catalog.to_csv(args.output, index=False)
    print(f"\nWrote {len(catalog)} rows to {args.output}", flush=True)


if __name__ == "__main__":
    main()
