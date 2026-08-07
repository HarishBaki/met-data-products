#!/usr/bin/env python3
"""
Download Ouranos CRCM5-CMIP6 subsets via THREDDS NCSS.

The simulation catalog is read from catalog_with_vars.csv by default (one row =
one NCML file; produced from catalog.csv by discover_vars.py, which is the only
script that touches catalog.csv directly). Files already present on disk are
skipped (no-clobber).

SELECTING PRODUCTS
------------------
  --index  0          single row
  --index  0,3,5      explicit list
  --index  11-15      inclusive range
  --index  0-5,10,23  mixed
  (omit)              all rows in the catalog

  Named filters (--source-id, --experiment, --variant, --realization) can be
  used instead of or combined with --index.

SELECTING VARIABLES
-------------------
  --vars               tas,pr,hurs    explicit comma-separated list
  --vars-file          path/to/vars.txt
                                       one var per line (or comma-separated),
                                       '#' starts a comment, blank lines ignored
  --vars-from-catalog  (flag, no value)
                                       per-row lookup of that row's own
                                       vars_{frequency} column, read directly
                                       off the loaded --catalog file (its
                                       default, catalog_with_vars.csv, already
                                       has these columns - see discover_vars.py).
                                       Every selected row gets its own list,
                                       built from what's actually available for
                                       that row rather than one list applied
                                       uniformly to every row.

  Exactly one of the three must be given - there is no built-in default variable
  list. Project-specific choices (e.g. the DFS downscaling pipeline's variable set)
  belong in the caller (a Slurm script, a vars-file, etc.), not in this script.

SELECTING FREQUENCY
--------------------
  --frequency  1hr / 3hr / day / mon   (default: 1hr)

TIME CHUNK MODES
----------------
  year     One output file per calendar year            (default)
  month    One output file per calendar month
  full     Entire simulation period in one request
  static   Single 1-hour anchor at sim start — for non-fx, time-invariant-in-practice
           fields only. fx variables (areacella, orog, sftlf, ...) are rejected here
           regardless of time-chunk mode - use download_fx.py for those instead.

SPATIAL MODES
-------------
  Default        Region bbox from configs/regions/{region}.yaml's grid.Ouranos.bbox,
                 selected via --region (default: New_York -> N=48 S=38 W=278 E=292,
                 preserved to match ~300GB of existing production data -- see that
                 file's header). New_York's bbox is NOT the DL-compatible/--round32
                 derivation the New_Mexico is; --region New_Mexico uses that.
  --full-domain  No spatial subsetting (full North American CORDEX domain)

OUTPUT ROOT
-----------
  --dest-root defaults to {data_root}/Ouranos_{region_tag} (cropped, from the region
  config) or OUTPUT_ROOT_FULL (--full-domain) - same split as download_fx.py -
  override with --dest-root if needed.

EXAMPLES
--------
  # List the catalog
  python download_ouranos.py --list

  # Download a single product by index (e.g. Slurm array task)
  python download_ouranos.py --index 2 --vars tas,hurs,huss,uas,vas,pr,ps --time-chunk year

  # Download CNRM-ESM2-1 historical monthly tas+pr for 1985-2014
  python download_ouranos.py --source-id CNRM-ESM2-1 --experiment historical \\
      --vars tas,pr --time-chunk month --start-year 1985 --end-year 2014

  # Download exactly what discover_vars.py found available for each row
  python download_ouranos.py --vars-from-catalog --time-chunk year

  # Download monthly-frequency data instead of the 1hr default
  python download_ouranos.py --index 2 --vars tas,pr --frequency mon --time-chunk full

  # fx variables (orog, areacella, sftlf, ...) - use download_fx.py, not this script
  python download_fx.py --index 0 --vars orog,areacella
"""

import argparse
import calendar
import concurrent.futures
import csv
import sys
import time
from datetime import date
from pathlib import Path
from urllib.parse import urlencode

import requests

_BOOTSTRAP_ROOT = Path(__file__).resolve().parents[1]
if str(_BOOTSTRAP_ROOT) not in sys.path:
    sys.path.insert(0, str(_BOOTSTRAP_ROOT))

from repo_utils import load_region_grid, load_region_vars

# ---------------------------------------------------------------------------
# THREDDS NCSS endpoint
# ---------------------------------------------------------------------------
BASE_NCSS_TMPL = (
    "https://pavics.ouranos.ca/twitcher/ows/proxy/thredds/ncss/grid"
    "/datasets/simulations/RCM-CMIP6/CORDEX/NAM-12/{freq}"
)

FREQUENCIES = ["1hr", "3hr", "day", "mon"]

# ---------------------------------------------------------------------------
# Spatial bounding boxes  (0-360 longitude convention)
# ---------------------------------------------------------------------------
# BBOX_NY is now historical/reference only -- kept because it's exactly what
# resolve_region_bbox("New_York") returns (configs/regions/New_York.yaml's
# grid.Ouranos.bbox was deliberately set to reproduce it, to protect the
# ~300GB of existing Ouranos_NYS production data -- see that file's header).
# Default spatial selection is now resolve_region_bbox(args.region), not this.
BBOX_NY   = {"north": 48, "south": 38, "west": 278, "east": 292}
BBOX_FULL = None  # omit spatial params → full domain


def resolve_region_bbox(region: str) -> dict:
    """NCSS bbox dict (north/south/west/east, 0-360 lon) from
    configs/regions/{region}.yaml's grid.Ouranos.bbox.

    Ouranos has no local crop step -- process_and_write_to_zarr.py does no
    spatial subsetting, so this download-time bbox param IS the actual crop
    mechanism (see compute_and_write_region_crop.py and New_York.yaml's header for how
    grid.Ouranos.bbox is derived per region, and why New York's deliberately
    preserves the historical BBOX_NY instead of the DL-compatible derivation
    New Mexico uses)."""
    region_grid = load_region_grid(region, "Ouranos")
    bbox = region_grid["bbox"]
    return {
        "north": bbox["lat_max"],
        "south": bbox["lat_min"],
        "west": (bbox["lon_min"] + 360) % 360,
        "east": (bbox["lon_max"] + 360) % 360,
    }


def resolve_region_output_root(region: str) -> Path:
    region_vars = load_region_vars(region)
    data_root = region_vars["data_root"]
    region_tag = region_vars["region_tag"]
    if not data_root:
        raise ValueError(f"configs/regions/{region}.yaml has no data_root set yet.")
    return Path(data_root) / f"Ouranos_{region_tag}"

# ---------------------------------------------------------------------------
# Output roots - full-domain output goes to RAW_DATA/Ouranos regardless of
# region (there's no region to speak of once nothing's cropped). Cropped
# output now defaults to resolve_region_output_root(args.region), not a
# fixed constant - see that function. Shared with download_fx.py so both
# scripts' full-domain destination moves together. (process_and_write_to_
# zarr.py's own pipeline stages year-by-year downloads through
# RAW_ROOT_DEFAULT/--raw-root instead, bypassing these CLI defaults
# entirely - see that script.)
# ---------------------------------------------------------------------------
# OUTPUT_ROOT_NYS is historical/reference only -- kept because it's exactly
# what resolve_region_output_root("New_York") returns.
OUTPUT_ROOT_NYS  = Path("/network/rit/lab/basulab/Projects/DFS/DATA/Ouranos_NYS")
OUTPUT_ROOT_FULL = Path("/network/rit/lab/basulab/RAW_DATA/Ouranos")

# fx (static) variables live on a separate raw THREDDS tree with version-dated
# paths - download_fx.py is the dedicated downloader for these (see its module
# docstring). Listed here, not there, so download_ouranos.py can refuse them
# without download_fx.py needing to import back into this module.
FX_VARS = [
    "areacella", "classFrac", "clayfrac", "cropFrac", "dtb", "fldcapacity",
    "grassFrac", "ksat", "ldpth", "mrsofc", "orog", "porosity", "rootd",
    "sandfrac", "sftgif", "sftlaf", "sftlf", "sftof", "sfturf",
    "treeFracPrimDec", "treeFracPrimEver", "wetlandFrac",
]

# ---------------------------------------------------------------------------
# Catalog I/O
# ---------------------------------------------------------------------------
DEFAULT_CATALOG = Path(__file__).parent / "catalog_with_vars.csv"


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
# Per-frequency NCML timerange strings
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


# ---------------------------------------------------------------------------
# Time-chunk helpers
# ---------------------------------------------------------------------------

def time_windows(chunk: str, sim_start_year: int, sim_end_year: int,
                 start_date: date | None, end_date: date | None):
    """Yield (time_start_iso, time_end_iso, label) tuples."""
    eff_start = start_date or date(sim_start_year, 1, 1)
    eff_end   = end_date   or date(sim_end_year,   12, 31)

    if chunk == "static":
        # Every catalog row's simulation starts Jan 1 of sim_start_year - the
        # static branch always anchors on 00:00Z/01:00Z regardless, so the
        # actual day-level start time (e.g. ERA5 v1-r2's :30 offset) never
        # mattered here even before this simplification.
        t = date(sim_start_year, 1, 1).isoformat()
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

def build_url(row: dict, vars_list: list[str], time_start: str, time_end: str,
              bbox: dict | None, frequency: str = "1hr") -> str:
    timerange = ncml_timerange_for_freq(row, frequency)
    ncml = (
        f"NAM-12_{row['source_id']}_{row['experiment_id']}_{row['variant']}"
        f"_OURANOS_CRCM5_{row['realization']}_{frequency}_{timerange}.ncml"
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
    base = BASE_NCSS_TMPL.format(freq=frequency)
    return f"{base}/{ncml}?{var_str}&{urlencode(params)}"


def build_dest(dest_root: Path, row: dict, vars_list: list[str],
               label: str, domain_tag: str, frequency: str = "1hr") -> Path:
    out_dir = dest_root / row["dest_subdir"]
    out_dir.mkdir(parents=True, exist_ok=True)
    var_tag    = "+".join(vars_list) if len(vars_list) <= 3 else f"{len(vars_list)}vars"
    fname = (
        f"NAM-12_{row['source_id']}_{row['experiment_id']}_{row['variant']}"
        f"_OURANOS_CRCM5_{row['realization']}_{frequency}_{domain_tag}_{var_tag}_{label}.nc4"
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
                   help="Path to catalog CSV (default: catalog_with_vars.csv next to this script)")
    p.add_argument("--list",        action="store_true",
                   help="Print the catalog and exit")

    # Product selection
    p.add_argument("--index",       default=None,
                   help="Row index(es) to process: single int, list (0,3,5), or range (11-15)")
    p.add_argument("--source-id",   default=None, help="Filter by source_id  (e.g. CNRM-ESM2-1)")
    p.add_argument("--experiment",  default=None, help="Filter by experiment  (e.g. historical)")
    p.add_argument("--variant",     default=None, help="Filter by variant     (e.g. r3i1p1f1)")
    p.add_argument("--realization", default=None, help="Filter by realization (e.g. v1-r2)")

    # Variable selection - exactly one source is required, no built-in default list
    var_source = p.add_mutually_exclusive_group(required=True)
    var_source.add_argument("--vars", default=None,
                   help="Comma-separated variable names, e.g. tas,pr,hurs")
    var_source.add_argument("--vars-file", default=None,
                   help="Path to a text file listing variables (one per line or "
                        "comma-separated; '#' comments and blank lines ignored)")
    var_source.add_argument("--vars-from-catalog", action="store_true",
                   help="Use each row's own vars_{frequency} column, read directly "
                        "off the loaded --catalog file (its default, "
                        "catalog_with_vars.csv, already has these columns - "
                        "see discover_vars.py)")

    # Frequency
    p.add_argument("--frequency",   default="1hr", choices=FREQUENCIES,
                   help="THREDDS time frequency to download (default: 1hr)")

    # Time controls
    p.add_argument("--time-chunk",  default="year",
                   choices=["year", "month", "full", "static"],
                   help="Time chunking mode (default: year)")
    p.add_argument("--start-date",  default=None, help="Start date YYYY-MM-DD")
    p.add_argument("--end-date",    default=None, help="End   date YYYY-MM-DD")
    p.add_argument("--start-year",  type=int, default=None, help="Start year (overridden by --start-date)")
    p.add_argument("--end-year",    type=int, default=None, help="End   year (overridden by --end-date)")

    # Spatial
    p.add_argument(
        "--region", type=str, default="New_York",
        help="Region config name under configs/regions/ (e.g. New_York, New_Mexico) -- "
             "supplies the bbox (grid.Ouranos.bbox) and, when --dest-root is not given "
             "explicitly, the output location (data_root/region_tag) too. Ignored if "
             "--full-domain is passed."
    )
    p.add_argument("--full-domain", action="store_true",
                   help="No spatial subsetting — full North American CORDEX domain")

    # Output
    p.add_argument("--dest-root",   default=None,
                   help="Root dir override, with dest_subdir from CSV appended. "
                        f"Default: {{data_root}}/Ouranos_{{region_tag}} (cropped, from --region) "
                        f"or {OUTPUT_ROOT_FULL} (--full-domain)")
    p.add_argument("--num-workers", type=int, default=4,
                   help="Parallel download threads (default: 4)")
    p.add_argument("--dry-run",     action="store_true",
                   help="Print tasks without downloading")
    return p.parse_args()


def resolve_vars(spec: str) -> list[str]:
    return [v.strip() for v in spec.split(",") if v.strip()]


def read_vars_file(path: Path) -> list[str]:
    out = []
    for line in path.read_text().splitlines():
        line = line.split("#", 1)[0].strip()
        if not line:
            continue
        out.extend(v.strip() for v in line.split(",") if v.strip())
    return out


def vars_from_catalog_row(row: dict, frequency: str, catalog_path: Path) -> list[str]:
    column = f"vars_{frequency}"
    cell = row.get(column, "")
    if not cell or cell == "ERROR":
        raise SystemExit(
            f"row {row['index']} has no usable {column} in {catalog_path} (value: {cell!r}) - "
            f"did you point --catalog at a catalog_with_vars.csv-style file?"
        )
    return [v for v in cell.split(";") if v]


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

    if args.full_domain:
        bbox = BBOX_FULL
        domain_tag = "full"
    else:
        bbox = resolve_region_bbox(args.region)
        domain_tag = load_region_vars(args.region)["region_tag"]

    if args.dest_root is not None:
        dest_root = Path(args.dest_root)
    else:
        dest_root = OUTPUT_ROOT_FULL if args.full_domain else resolve_region_output_root(args.region)
    start_date, end_date = resolve_dates(args)

    tasks = []
    vars_by_row = {}
    for row in rows:
        if args.vars is not None:
            vars_list = resolve_vars(args.vars)
        elif args.vars_file is not None:
            vars_list = read_vars_file(Path(args.vars_file))
        else:
            vars_list = vars_from_catalog_row(row, args.frequency, Path(args.catalog))

        fx_requested = sorted(set(vars_list) & set(FX_VARS))
        if fx_requested:
            raise SystemExit(
                f"{fx_requested} are fx (static) variables, not time-varying NCML data - "
                f"download_ouranos.py can't resolve them. Use download_fx.py instead, e.g.:\n"
                f"  python download_fx.py --index {row['index']} --vars {','.join(fx_requested)}"
            )
        vars_by_row[row["index"]] = vars_list

        for t_start, t_end, label in time_windows(
            args.time_chunk, row["sim_start_year"], row["sim_end_year"],
            start_date, end_date,
        ):
            url  = build_url(row, vars_list, t_start, t_end, bbox, args.frequency)
            path = build_dest(dest_root, row, vars_list, label, domain_tag, args.frequency)
            tasks.append((url, path))

    print(f"Matched rows : {[r['index'] for r in rows]}", flush=True)
    print(f"Total tasks  : {len(tasks)}",                  flush=True)
    if len(set(map(tuple, vars_by_row.values()))) == 1:
        print(f"Variables    : {next(iter(vars_by_row.values()))}", flush=True)
    else:
        print("Variables    : (per-row, see vars-from-catalog)", flush=True)
        for idx, vlist in vars_by_row.items():
            print(f"  row {idx}: {vlist}", flush=True)
    print(f"Frequency    : {args.frequency}",              flush=True)
    print(f"Time chunk   : {args.time_chunk}",             flush=True)
    print(f"Spatial      : {'full domain' if args.full_domain else f'{args.region} bbox {bbox}'}", flush=True)

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
