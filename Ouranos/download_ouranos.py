#!/usr/bin/env python3
"""
Download Ouranos CRCM5-CMIP6 subsets via THREDDS NCSS.

The simulation catalog is read from catalog.csv (one row = one NCML file).
Files already present on disk are skipped (no-clobber).

SELECTING PRODUCTS
------------------
  --index  0          single row
  --index  0,3,5      explicit list
  --index  11-15      inclusive range
  --index  0-5,10,23  mixed
  (omit)              all rows in the catalog

  Named filters (--source-id, --experiment, --variant, --realization) can be
  used instead of or combined with --index.

VARIABLE PRESETS
----------------
  all      areacella orog tas hurs huss uas vas pr ps   (default)
  static   areacella orog                               (for grid inspection)
  dynamic  tas hurs huss uas vas pr ps

TIME CHUNK MODES
----------------
  year     One output file per calendar year            (default)
  month    One output file per calendar month
  full     Entire simulation period in one request
  static   Single 1-hour anchor at sim start — pair with --vars static

SPATIAL MODES
-------------
  Default     NY bbox: N=48 S=38 W=278 E=292  (0-360 lon)
  --full-domain  No spatial subsetting (full North American CORDEX domain)

EXAMPLES
--------
  # List the catalog
  python download_ouranos.py --list

  # Download orography for every simulation (grid comparison)
  python download_ouranos.py --vars static --time-chunk static --full-domain

  # Download a single product by index (e.g. Slurm array task)
  python download_ouranos.py --index 2 --vars dynamic --time-chunk year

  # Download CNRM-ESM2-1 historical monthly tas+pr for 1985-2014
  python download_ouranos.py --source-id CNRM-ESM2-1 --experiment historical \\
      --vars tas,pr --time-chunk month --start-year 1985 --end-year 2014

  # Dry run
  python download_ouranos.py --index 11-15 --vars static --time-chunk static --dry-run
"""

import argparse
import calendar
import concurrent.futures
import csv
import time
from datetime import date
from pathlib import Path
from urllib.parse import urlencode

import requests

# ---------------------------------------------------------------------------
# THREDDS NCSS endpoint
# ---------------------------------------------------------------------------
BASE_NCSS = (
    "https://pavics.ouranos.ca/twitcher/ows/proxy/thredds/ncss/grid"
    "/datasets/simulations/RCM-CMIP6/CORDEX/NAM-12/1hr"
)

# ---------------------------------------------------------------------------
# Spatial bounding boxes  (0-360 longitude convention)
# ---------------------------------------------------------------------------
BBOX_NY   = {"north": 48, "south": 38, "west": 278, "east": 292}
BBOX_FULL = None  # omit spatial params → full domain

# ---------------------------------------------------------------------------
# Variable presets
# ---------------------------------------------------------------------------
VAR_PRESETS = {
    "static":  ["areacella", "orog"],
    "dynamic": ["tas", "hurs", "huss", "uas", "vas", "pr", "ps"],
    "all":     ["areacella", "orog", "tas", "hurs", "huss", "uas", "vas", "pr", "ps"],
}

# ---------------------------------------------------------------------------
# Catalog I/O
# ---------------------------------------------------------------------------
DEFAULT_CATALOG = Path(__file__).parent / "catalog.csv"


def load_catalog(csv_path: Path) -> list[dict]:
    with open(csv_path, newline="") as f:
        rows = list(csv.DictReader(f))
    for r in rows:
        r["index"]          = int(r["index"])
        r["sim_start_year"] = int(r["sim_start_year"])
        r["sim_end_year"]   = int(r["sim_end_year"])
    return rows


def parse_index_spec(spec: str, max_index: int) -> set[int]:
    """Parse '0', '0,3,5', '11-15', or '0-5,10,23' into a set of ints."""
    indices = set()
    for part in spec.split(","):
        part = part.strip()
        if "-" in part:
            lo, hi = part.split("-", 1)
            indices.update(range(int(lo), int(hi) + 1))
        else:
            indices.add(int(part))
    out_of_range = [i for i in indices if i < 0 or i > max_index]
    if out_of_range:
        raise ValueError(f"Index out of range (0-{max_index}): {out_of_range}")
    return indices


# ---------------------------------------------------------------------------
# Time-chunk helpers
# ---------------------------------------------------------------------------

def _sim_start_date(ncml_timerange: str) -> date:
    s = ncml_timerange[:8]
    return date(int(s[:4]), int(s[4:6]), int(s[6:8]))


def time_windows(chunk: str, sim_start_year: int, sim_end_year: int,
                 ncml_timerange: str, start_date: date | None, end_date: date | None):
    """Yield (time_start_iso, time_end_iso, label) tuples."""
    eff_start = start_date or date(sim_start_year, 1, 1)
    eff_end   = end_date   or date(sim_end_year,   12, 31)

    if chunk == "static":
        anchor = _sim_start_date(ncml_timerange)
        t = anchor.isoformat()
        yield f"{t}T00:00:00Z", f"{t}T01:00:00Z", "static"

    elif chunk == "full":
        yield (
            f"{eff_start.isoformat()}T00:00:00Z",
            f"{eff_end.isoformat()}T23:00:00Z",
            f"{eff_start.year}-{eff_end.year}",
        )

    elif chunk == "year":
        for yr in range(eff_start.year, eff_end.year + 1):
            y_start = max(date(yr, 1,  1),  eff_start)
            y_end   = min(date(yr, 12, 31), eff_end)
            yield f"{y_start.isoformat()}T00:00:00Z", f"{y_end.isoformat()}T23:00:00Z", str(yr)

    elif chunk == "month":
        cur = date(eff_start.year, eff_start.month, 1)
        while cur <= eff_end:
            last_day = calendar.monthrange(cur.year, cur.month)[1]
            m_start  = max(cur, eff_start)
            m_end    = min(date(cur.year, cur.month, last_day), eff_end)
            yield f"{m_start.isoformat()}T00:00:00Z", f"{m_end.isoformat()}T23:00:00Z", cur.strftime("%Y-%m")
            cur = date(cur.year + (cur.month == 12), (cur.month % 12) + 1, 1)

    else:
        raise ValueError(f"Unknown time-chunk mode: {chunk!r}")


# ---------------------------------------------------------------------------
# URL and path construction
# ---------------------------------------------------------------------------

def build_url(row: dict, vars_list: list[str],
              time_start: str, time_end: str, bbox: dict | None) -> str:
    ncml = (
        f"NAM-12_{row['source_id']}_{row['experiment_id']}_{row['variant']}"
        f"_OURANOS_CRCM5_{row['realization']}_1hr_{row['ncml_timerange']}.ncml"
    )
    var_str = "&".join(f"var={v}" for v in vars_list)
    params  = {
        "horizStride": 1,
        "time_start":  time_start,
        "time_end":    time_end,
        "accept":      "netcdf4ext",
        "addLatLon":   "true",
    }
    if bbox:
        params.update(north=bbox["north"], south=bbox["south"],
                      west=bbox["west"],   east=bbox["east"])
    return f"{BASE_NCSS}/{ncml}?{var_str}&{urlencode(params)}"


def build_dest(dest_root: Path, row: dict, vars_list: list[str],
               label: str, full_domain: bool) -> Path:
    out_dir = dest_root / row["dest_subdir"]
    out_dir.mkdir(parents=True, exist_ok=True)
    var_tag    = "+".join(vars_list) if len(vars_list) <= 3 else f"{len(vars_list)}vars"
    domain_tag = "full" if full_domain else "NYS"
    fname = (
        f"NAM-12_{row['source_id']}_{row['experiment_id']}_{row['variant']}"
        f"_OURANOS_CRCM5_{row['realization']}_{domain_tag}_{var_tag}_{label}.nc4"
    )
    return out_dir / fname


# ---------------------------------------------------------------------------
# Download worker
# ---------------------------------------------------------------------------

def download_one(url: str, path: Path, retries: int = 3, timeout: int = 3600) -> str:
    if path.exists():
        return f"SKIP  {path.name}"
    tmp = path.with_suffix(".tmp")
    for attempt in range(1, retries + 1):
        try:
            with requests.get(url, stream=True, timeout=timeout) as r:
                r.raise_for_status()
                with open(tmp, "wb") as f:
                    for chunk in r.iter_content(chunk_size=1 << 20):
                        f.write(chunk)
            tmp.rename(path)
            return f"OK    {path.name}"
        except Exception as exc:
            tmp.unlink(missing_ok=True)
            if attempt < retries:
                wait = 30 * attempt
                print(f"  attempt {attempt} failed ({exc}); retrying in {wait}s", flush=True)
                time.sleep(wait)
            else:
                return f"FAIL  {path.name}  [{exc}]"


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def parse_args():
    p = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    # Catalog
    p.add_argument("--catalog",     default=str(DEFAULT_CATALOG),
                   help="Path to catalog CSV (default: catalog.csv next to this script)")
    p.add_argument("--list",        action="store_true",
                   help="Print the catalog and exit")

    # Product selection
    p.add_argument("--index",       default=None,
                   help="Row index(es) to process: single int, list (0,3,5), or range (11-15)")
    p.add_argument("--source-id",   default=None, help="Filter by source_id  (e.g. CNRM-ESM2-1)")
    p.add_argument("--experiment",  default=None, help="Filter by experiment  (e.g. historical)")
    p.add_argument("--variant",     default=None, help="Filter by variant     (e.g. r3i1p1f1)")
    p.add_argument("--realization", default=None, help="Filter by realization (e.g. v1-r2)")

    # Variable selection
    p.add_argument("--vars", default="all",
                   help="Comma-separated variables or preset: all / static / dynamic")

    # Time controls
    p.add_argument("--time-chunk",  default="year",
                   choices=["year", "month", "full", "static"],
                   help="Time chunking mode (default: year)")
    p.add_argument("--start-date",  default=None, help="Start date YYYY-MM-DD")
    p.add_argument("--end-date",    default=None, help="End   date YYYY-MM-DD")
    p.add_argument("--start-year",  type=int, default=None, help="Start year (overridden by --start-date)")
    p.add_argument("--end-year",    type=int, default=None, help="End   year (overridden by --end-date)")

    # Spatial
    p.add_argument("--full-domain", action="store_true",
                   help="No spatial subsetting — full North American CORDEX domain")

    # Output
    p.add_argument("--dest-root",   default="raw",
                   help="Root directory prepended to dest_subdir from CSV (default: ./raw)")
    p.add_argument("--num-workers", type=int, default=4,
                   help="Parallel download threads (default: 4)")
    p.add_argument("--dry-run",     action="store_true",
                   help="Print tasks without downloading")
    return p.parse_args()


def resolve_vars(spec: str) -> list[str]:
    spec = spec.strip()
    if spec in VAR_PRESETS:
        return VAR_PRESETS[spec]
    return [v.strip() for v in spec.split(",") if v.strip()]


def resolve_dates(args) -> tuple[date | None, date | None]:
    start = date.fromisoformat(args.start_date) if args.start_date else \
            (date(args.start_year, 1, 1)  if args.start_year else None)
    end   = date.fromisoformat(args.end_date)   if args.end_date   else \
            (date(args.end_year, 12, 31)  if args.end_year   else None)
    return start, end


def main():
    args      = parse_args()
    catalog   = load_catalog(Path(args.catalog))

    # --list
    if args.list:
        header = f"{'idx':>3}  {'source_id':<18} {'experiment':<12} {'variant':<12} {'real':<6}  {'years'}"
        print(header)
        print("-" * len(header))
        for r in catalog:
            print(f"{r['index']:>3}  {r['source_id']:<18} {r['experiment_id']:<12} "
                  f"{r['variant']:<12} {r['realization']:<6}  "
                  f"{r['sim_start_year']}-{r['sim_end_year']}")
        return

    # Filter rows
    rows = catalog
    if args.index is not None:
        wanted = parse_index_spec(args.index, max_index=catalog[-1]["index"])
        rows   = [r for r in rows if r["index"] in wanted]
    if args.source_id:   rows = [r for r in rows if r["source_id"]    == args.source_id]
    if args.experiment:  rows = [r for r in rows if r["experiment_id"] == args.experiment]
    if args.variant:     rows = [r for r in rows if r["variant"]       == args.variant]
    if args.realization: rows = [r for r in rows if r["realization"]   == args.realization]

    if not rows:
        print("No catalog rows matched the given filters.")
        return

    vars_list   = resolve_vars(args.vars)
    bbox        = BBOX_FULL if args.full_domain else BBOX_NY
    dest_root   = Path(args.dest_root)
    start_date, end_date = resolve_dates(args)

    tasks = []
    for row in rows:
        for t_start, t_end, label in time_windows(
            args.time_chunk, row["sim_start_year"], row["sim_end_year"],
            row["ncml_timerange"], start_date, end_date,
        ):
            url  = build_url(row, vars_list, t_start, t_end, bbox)
            path = build_dest(dest_root, row, vars_list, label, args.full_domain)
            tasks.append((url, path))

    print(f"Matched rows : {[r['index'] for r in rows]}", flush=True)
    print(f"Total tasks  : {len(tasks)}",                  flush=True)
    print(f"Variables    : {vars_list}",                   flush=True)
    print(f"Time chunk   : {args.time_chunk}",             flush=True)
    print(f"Spatial      : {'full domain' if args.full_domain else f'NYS bbox {BBOX_NY}'}", flush=True)

    if args.dry_run:
        for url, path in tasks:
            print(f"  {path}\n    <- {url}")
        return

    with concurrent.futures.ThreadPoolExecutor(max_workers=args.num_workers) as pool:
        futures = {pool.submit(download_one, url, path): path for url, path in tasks}
        for i, fut in enumerate(concurrent.futures.as_completed(futures), 1):
            print(f"[{i:>5}/{len(tasks)}] {fut.result()}", flush=True)


if __name__ == "__main__":
    main()
