#!/usr/bin/env python3
"""
Discover, per catalog row and per time frequency, which CORDEX/CMIP6 variables
are genuinely time-varying on the Ouranos THREDDS server.

Each simulation's .ncml aggregation folds in static "fx" grid-info variables
(areacella, orog, sftlf, classFrac, ...) alongside the real per-frequency
output. Opening the dataset over OPeNDAP (metadata only, no data download)
and checking each variable's dims for a "time" axis cleanly separates the two.

Variable availability differs per row (not just per frequency), so discovery
is done independently for every (row, frequency) pair - never assumed shared.

Output: a copy of catalog.csv with 4 new columns (vars_1hr, vars_3hr, vars_day,
vars_mon), each a ';'-separated, alphabetically sorted list of variable names,
or 'ERROR' if discovery failed after retries. The input catalog is never
modified in place.
"""

import argparse
import csv
import time
from datetime import date
from pathlib import Path

import xarray as xr

from download_ouranos import DEFAULT_CATALOG, load_catalog, parse_index_spec

FREQUENCIES = ["1hr", "3hr", "day", "mon"]

BASE_OPENDAP = (
    "https://pavics.ouranos.ca/twitcher/ows/proxy/thredds/dodsC"
    "/datasets/simulations/RCM-CMIP6/CORDEX/NAM-12/{freq}"
)

NON_PRODUCT_VARS = {"time_bnds", "poids", "time_vectors"}

VAR_COLUMNS = [f"vars_{freq}" for freq in FREQUENCIES]


# ---------------------------------------------------------------------------
# URL construction
# ---------------------------------------------------------------------------

def ncml_timerange_for_freq(row: dict, freq: str) -> str:
    if freq == "1hr":
        # catalog.csv's ncml_timerange is the verified, verbatim 1hr string -
        # some rows (e.g. ERA5 v1-r2) are anchored at :30 instead of :00, so
        # this must be reused rather than recomputed from sim_start/end_year.
        return row["ncml_timerange"]

    start = date(row["sim_start_year"], 1, 1)
    end = date(row["sim_end_year"], 12, 31)
    if freq == "3hr":
        return f"{start:%Y%m%d}0000-{end:%Y%m%d}2100"
    if freq == "day":
        return f"{start:%Y%m%d}-{end:%Y%m%d}"
    if freq == "mon":
        return f"{start:%Y%m}-{end:%Y%m}"
    raise ValueError(f"Unknown frequency: {freq!r}")


def build_ncml_url(row: dict, freq: str) -> str:
    timerange = ncml_timerange_for_freq(row, freq)
    ncml = (
        f"NAM-12_{row['source_id']}_{row['experiment_id']}_{row['variant']}"
        f"_OURANOS_CRCM5_{row['realization']}_{freq}_{timerange}.ncml"
    )
    base = BASE_OPENDAP.format(freq=freq)
    return f"{base}/{ncml}"


# ---------------------------------------------------------------------------
# Discovery
# ---------------------------------------------------------------------------

def discover_one(url: str, retries: int = 3, backoff_base: int = 15) -> tuple[str, list[str] | None, str | None]:
    """Returns (status, var_list_or_None, error_message_or_None)."""
    for attempt in range(1, retries + 1):
        try:
            ds = xr.open_dataset(url)
            dynamic_vars = sorted(
                v for v in ds.data_vars
                if "time" in ds[v].dims and v not in NON_PRODUCT_VARS
            )
            ds.close()
            return "OK", dynamic_vars, None
        except Exception as exc:
            if attempt < retries:
                wait = backoff_base * attempt
                print(f"    attempt {attempt} failed ({exc}); retrying in {wait}s", flush=True)
                time.sleep(wait)
            else:
                return "FAIL", None, str(exc)
    return "FAIL", None, "unreachable"


def discover_all(rows: list[dict], frequencies: list[str], retries: int) -> dict[tuple[int, str], dict]:
    results = {}
    for row in rows:
        for freq in frequencies:
            url = build_ncml_url(row, freq)
            status, vars_, err = discover_one(url, retries=retries)
            results[(row["index"], freq)] = {"status": status, "vars": vars_, "error": err, "url": url}
            tag = f"row={row['index']:>2} {row['source_id']}/{row['experiment_id']} freq={freq}"
            if status == "OK":
                if vars_:
                    print(f"  OK    {tag}  ({len(vars_)} vars)", flush=True)
                else:
                    print(f"  WARN  {tag}  (0 vars - unexpected, check manually)", flush=True)
            else:
                print(f"  FAIL  {tag}  [{err}]", flush=True)
    return results


# ---------------------------------------------------------------------------
# CSV merge / output
# ---------------------------------------------------------------------------

def cell_value(entry: dict | None) -> str:
    if entry is None:
        return ""
    if entry["status"] == "OK":
        return ";".join(entry["vars"])
    return "ERROR"


def load_base_rows(output_path: Path, catalog_path: Path) -> tuple[list[dict], list[str]]:
    """Load existing --output (to accumulate across partial runs) if present,
    otherwise the original catalog. Returns (rows, fieldnames)."""
    if output_path.exists():
        with open(output_path, newline="") as f:
            reader = csv.DictReader(f)
            rows = list(reader)
            fieldnames = reader.fieldnames
        return rows, fieldnames

    with open(catalog_path, newline="") as f:
        reader = csv.DictReader(f)
        rows = list(reader)
        fieldnames = reader.fieldnames
    for col in VAR_COLUMNS:
        if col not in fieldnames:
            fieldnames = fieldnames + [col]
    for r in rows:
        for col in VAR_COLUMNS:
            r.setdefault(col, "")
    return rows, fieldnames


def merge_results(rows: list[dict], results: dict[tuple[int, str], dict]) -> None:
    for r in rows:
        idx = int(r["index"])
        for freq in FREQUENCIES:
            entry = results.get((idx, freq))
            if entry is not None:
                r[f"vars_{freq}"] = cell_value(entry)


def write_rows(rows: list[dict], fieldnames: list[str], output_path: Path) -> None:
    with open(output_path, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def parse_args():
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--catalog", default=str(DEFAULT_CATALOG), help="Path to input catalog CSV")
    p.add_argument("--output", default=None,
                   help="Path to write the catalog with vars_* columns (default: catalog_with_vars.csv next to --catalog)")

    p.add_argument("--index", default=None, help="Row index(es): single int, list (0,3,5), or range (11-15)")
    p.add_argument("--source-id", default=None)
    p.add_argument("--experiment", default=None)
    p.add_argument("--variant", default=None)
    p.add_argument("--realization", default=None)

    p.add_argument("--frequencies", default=",".join(FREQUENCIES),
                   help=f"Comma-separated subset of {FREQUENCIES} (default: all)")
    p.add_argument("--retries", type=int, default=3)
    p.add_argument("--dry-run", action="store_true", help="Print URLs without opening them")
    return p.parse_args()


def main():
    args = parse_args()
    catalog_path = Path(args.catalog)
    output_path = Path(args.output) if args.output else catalog_path.with_name("catalog_with_vars.csv")

    if output_path.resolve() == catalog_path.resolve():
        raise SystemExit("--output must not be the same file as --catalog (refusing to overwrite the input).")

    catalog = load_catalog(catalog_path)

    rows = catalog
    if args.index is not None:
        wanted = parse_index_spec(args.index, max_index=catalog[-1]["index"])
        rows = [r for r in rows if r["index"] in wanted]
    if args.source_id:   rows = [r for r in rows if r["source_id"]    == args.source_id]
    if args.experiment:  rows = [r for r in rows if r["experiment_id"] == args.experiment]
    if args.variant:     rows = [r for r in rows if r["variant"]       == args.variant]
    if args.realization: rows = [r for r in rows if r["realization"]   == args.realization]

    if not rows:
        print("No catalog rows matched the given filters.")
        return

    frequencies = [f.strip() for f in args.frequencies.split(",") if f.strip()]
    unknown = set(frequencies) - set(FREQUENCIES)
    if unknown:
        raise SystemExit(f"Unknown frequency/frequencies: {sorted(unknown)} (valid: {FREQUENCIES})")

    print(f"Matched rows : {[r['index'] for r in rows]}", flush=True)
    print(f"Frequencies  : {frequencies}", flush=True)

    if args.dry_run:
        for row in rows:
            for freq in frequencies:
                print(f"  row={row['index']:>2} freq={freq}  {build_ncml_url(row, freq)}")
        return

    results = discover_all(rows, frequencies, retries=args.retries)

    ok = sum(1 for v in results.values() if v["status"] == "OK" and v["vars"])
    warn = sum(1 for v in results.values() if v["status"] == "OK" and not v["vars"])
    fail = sum(1 for v in results.values() if v["status"] == "FAIL")
    print(f"\nSummary: OK={ok} WARN={warn} FAIL={fail}", flush=True)
    if fail:
        failed = [k for k, v in results.items() if v["status"] == "FAIL"]
        print(f"Failed (index, freq) pairs: {failed}", flush=True)

    base_rows, fieldnames = load_base_rows(output_path, catalog_path)
    merge_results(base_rows, results)
    write_rows(base_rows, fieldnames, output_path)
    print(f"\nWrote {output_path}", flush=True)


if __name__ == "__main__":
    main()
