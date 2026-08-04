#!/usr/bin/env python3
"""
Download static (fx) CRCM5-CMIP6 variables for every catalog row via THREDDS NCSS.

Unlike download_ouranos.py's time-varying variables - which come from curated
NCML aggregations under .../NAM-12/{1hr,3hr,day,mon} and can be requested by a
constructed URL - fx variables live in a separate raw THREDDS tree per
(source_id, experiment_id, variant, realization), and the actual filename sits
behind an unpredictable version-dated subfolder that must be discovered by
walking THREDDS catalog.xml twice:

    fx/{var}/catalog.xml           -> one <catalogRef> naming the version
    fx/{var}/{version}/catalog.xml -> one <dataset urlPath="...">

Version dates differ per (row, var) - even across experiments of the same
source_id/variant/realization - so every row is downloaded independently, no
deduplication. Once the urlPath is resolved, NCSS grid subsetting works on the
standalone file exactly like it does on the curated aggregations.

VARIABLES
---------
  all (default)   areacella classFrac clayfrac cropFrac dtb fldcapacity
                   grassFrac ksat ldpth mrsofc orog porosity rootd sandfrac
                   sftgif sftlaf sftlf sftof sfturf treeFracPrimDec
                   treeFracPrimEver wetlandFrac

SPATIAL MODES
-------------
  Default        Region bbox from configs/regions/{region}.yaml's grid.Ouranos.bbox,
                 selected via --region (default: New_York) - same as download_ouranos.py
  --full-domain  No spatial subsetting (full North American CORDEX domain)

EXAMPLES
--------
  # Discover URLs without downloading
  python download_fx.py --index 0 --vars orog --dry-run

  # Download all 22 fx vars for one row
  python download_fx.py --index 23

  # Download orog + areacella for every row, full domain
  python download_fx.py --vars orog,areacella --full-domain
"""

import argparse
import concurrent.futures
import time
import xml.etree.ElementTree as ET
from pathlib import Path
from urllib.parse import urlencode, urljoin

import requests

from download_ouranos import (
    BBOX_FULL,
    DEFAULT_CATALOG,
    FX_VARS,
    OUTPUT_ROOT_FULL,
    download_one,
    load_catalog,
    load_region_vars,
    parse_index_spec,
    resolve_region_bbox,
    resolve_region_output_root,
)

# ---------------------------------------------------------------------------
# THREDDS endpoints
# ---------------------------------------------------------------------------
BASE_CATALOG = (
    "https://pavics.ouranos.ca/twitcher/ows/proxy/thredds/catalog"
    "/birdhouse/disk2/ouranos/CORDEX/CMIP6/DD/NAM-12/OURANOS"
)
BASE_NCSS_FX = "https://pavics.ouranos.ca/twitcher/ows/proxy/thredds/ncss/grid"
BASE_FILESERVER_FX = "https://pavics.ouranos.ca/twitcher/ows/proxy/thredds/fileServer"

XLINK_HREF = "{http://www.w3.org/1999/xlink}href"


# ---------------------------------------------------------------------------
# Discovery: walk catalog.xml twice to resolve the version-dated urlPath
# ---------------------------------------------------------------------------

def fx_var_catalog_url(row: dict, var: str) -> str:
    return (
        f"{BASE_CATALOG}/{row['source_id']}/{row['experiment_id']}/{row['variant']}"
        f"/CRCM5/{row['realization']}/fx/{var}/catalog.xml"
    )


def _fetch_xml(url: str, retries: int, backoff_base: int) -> ET.Element | None:
    for attempt in range(1, retries + 1):
        try:
            resp = requests.get(url, timeout=30)
            resp.raise_for_status()
            return ET.fromstring(resp.content)
        except (requests.exceptions.RequestException, ET.ParseError) as exc:
            if attempt < retries:
                wait = backoff_base * attempt
                print(f"    xml fetch attempt {attempt} failed ({exc}); retrying in {wait}s", flush=True)
                time.sleep(wait)
            else:
                return None
    return None


def resolve_urlpath(row: dict, var: str, retries: int = 3, backoff_base: int = 10) -> tuple[str | None, str | None]:
    """Returns (urlPath, error). urlPath is None on failure."""
    var_catalog_url = fx_var_catalog_url(row, var)
    root = _fetch_xml(var_catalog_url, retries, backoff_base)
    if root is None:
        return None, f"could not fetch/parse {var_catalog_url}"

    catalog_refs = root.findall(".//{*}catalogRef")
    if len(catalog_refs) != 1:
        return None, f"expected 1 catalogRef under fx/{var}, found {len(catalog_refs)}"

    href = catalog_refs[0].get(XLINK_HREF)
    if not href:
        return None, f"catalogRef for fx/{var} missing xlink:href"
    version_catalog_url = urljoin(var_catalog_url, href)

    root2 = _fetch_xml(version_catalog_url, retries, backoff_base)
    if root2 is None:
        return None, f"could not fetch/parse {version_catalog_url}"

    # findall(".//{*}dataset") matches both the wrapping folder-level <dataset>
    # and the actual file's <dataset urlPath=...> (the file is nested inside the
    # folder element) - only the latter carries urlPath, so filter on that
    # rather than assuming a single match.
    datasets = [d for d in root2.findall(".//{*}dataset") if d.get("urlPath")]
    if len(datasets) != 1:
        return None, f"expected 1 dataset with urlPath under fx/{var}/<version>, found {len(datasets)}"

    return datasets[0].get("urlPath"), None


def discover_all(rows: list[dict], vars_list: list[str], retries: int) -> dict[tuple[int, str], dict]:
    results = {}
    for row in rows:
        for var in vars_list:
            url_path, err = resolve_urlpath(row, var, retries=retries)
            results[(row["index"], var)] = {"urlPath": url_path, "error": err}
            tag = f"row={row['index']:>2} {row['source_id']}/{row['experiment_id']} var={var}"
            if url_path:
                print(f"  OK    {tag}", flush=True)
            else:
                print(f"  FAIL  {tag}  [{err}]", flush=True)
    return results


# ---------------------------------------------------------------------------
# NCSS URL + destination path construction
# ---------------------------------------------------------------------------

def build_fx_url(url_path: str, var: str, bbox: dict | None) -> str:
    params = {"var": var, "accept": "netcdf4ext", "addLatLon": "true"}
    if bbox:
        params.update(north=bbox["north"], south=bbox["south"],
                      west=bbox["west"], east=bbox["east"])
    return f"{BASE_NCSS_FX}/{url_path}?{urlencode(params)}"


def build_fileserver_url(url_path: str) -> str:
    """Full file, no NCSS subsetting - fallback for variables NCSS can't grid-subset
    (e.g. rootd, which carries an extra non-spatial 'vegetation_type' dimension that
    THREDDS's NCSS grid service can't handle, even at full domain)."""
    return f"{BASE_FILESERVER_FX}/{url_path}"


def build_fx_dest(dest_root: Path, row: dict, var: str, domain_tag: str) -> Path:
    out_dir = dest_root / row["dest_subdir"]
    out_dir.mkdir(parents=True, exist_ok=True)
    fname = f"{row['realization']}_{domain_tag.lower()}_{var}.nc4"
    return out_dir / fname


def download_with_fallback(
    url: str, fallback_url: str, dest_root: Path, full_domain_root: Path,
    row: dict, var: str, domain_tag: str, retries: int,
) -> str:
    """Try the NCSS-subsetted URL first; if it fails outright (not just slow/flaky),
    fall back to a full-domain plain-file download so a single NCSS-incompatible
    variable (e.g. rootd - see build_fileserver_url) doesn't end up missing entirely.
    The fallback lands under full_domain_root at the 'full' domain path, not the
    originally requested dest_root/domain_tag, since that's honestly what its
    content is and where uncropped data belongs."""
    path = build_fx_dest(dest_root, row, var, domain_tag)
    result = download_one(url, path, retries=retries)
    if not result.startswith("FAIL"):
        return result

    fallback_path = build_fx_dest(full_domain_root, row, var, "full")
    fallback_result = download_one(fallback_url, fallback_path, retries=retries)
    if fallback_result.startswith("OK"):
        return f"OK    {fallback_path.name}  [full-domain fallback - NCSS subsetting failed]"
    if fallback_result.startswith("SKIP"):
        return f"SKIP  {fallback_path.name}  [full-domain fallback already on disk]"
    return f"FAIL  {path.name}  [NCSS and full-domain fallback both failed]"


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def parse_args():
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--catalog", default=str(DEFAULT_CATALOG))
    p.add_argument("--index", default=None, help="Row index(es): single int, list (0,3,5), or range (11-15)")
    p.add_argument("--source-id", default=None)
    p.add_argument("--experiment", default=None)
    p.add_argument("--variant", default=None)
    p.add_argument("--realization", default=None)

    p.add_argument("--vars", default="all", help="Comma-separated fx variables, or 'all' (default: all 22)")
    p.add_argument(
        "--region", type=str, default="New_York",
        help="Region config name under configs/regions/ (e.g. New_York, New_Mexico) -- "
             "supplies the bbox (grid.Ouranos.bbox) and, when --dest-root is not given "
             "explicitly, the output location (data_root/region_tag) too. Ignored if "
             "--full-domain is passed."
    )
    p.add_argument("--full-domain", action="store_true",
                   help="No spatial subsetting - full North American CORDEX domain")

    p.add_argument("--dest-root", default=None,
                   help="Root dir override; files land at {dest-root}/{dest_subdir}/{realization}_{region_tag|full}_{var}.nc4. "
                        f"Default: {{data_root}}/Ouranos_{{region_tag}} (cropped, from --region) "
                        f"or {OUTPUT_ROOT_FULL} (--full-domain)")
    p.add_argument("--num-workers", type=int, default=4, help="Parallel download threads (default: 4)")
    p.add_argument("--retries", type=int, default=3)
    p.add_argument("--dry-run", action="store_true",
                   help="Print discovered tasks without downloading (discovery still runs)")
    return p.parse_args()


def resolve_vars(spec: str) -> list[str]:
    spec = spec.strip()
    if spec == "all":
        return FX_VARS
    return [v.strip() for v in spec.split(",") if v.strip()]


def main():
    args = parse_args()
    catalog = load_catalog(Path(args.catalog))

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

    vars_list = resolve_vars(args.vars)
    if args.full_domain:
        bbox = BBOX_FULL
        domain_tag = "full"
    else:
        bbox = resolve_region_bbox(args.region)
        domain_tag = load_region_vars(args.region)["region_tag"]

    if args.dest_root is not None:
        dest_root = full_domain_root = Path(args.dest_root)
    else:
        dest_root = OUTPUT_ROOT_FULL if args.full_domain else resolve_region_output_root(args.region)
        full_domain_root = OUTPUT_ROOT_FULL

    print(f"Matched rows : {[r['index'] for r in rows]}", flush=True)
    print(f"Variables    : {vars_list}", flush=True)
    print(f"Spatial      : {'full domain' if args.full_domain else f'{args.region} bbox {bbox}'}", flush=True)
    print(f"\nResolving {len(rows) * len(vars_list)} (row, var) urlPaths...", flush=True)

    results = discover_all(rows, vars_list, retries=args.retries)
    disc_ok = sum(1 for v in results.values() if v["urlPath"])
    disc_fail = len(results) - disc_ok
    print(f"\nDiscovery: OK={disc_ok} FAIL={disc_fail}", flush=True)
    if disc_fail:
        failed = [k for k, v in results.items() if not v["urlPath"]]
        print(f"Failed (index, var) pairs: {failed}", flush=True)

    tasks = []
    for row in rows:
        for var in vars_list:
            entry = results[(row["index"], var)]
            if entry["urlPath"] is None:
                continue
            url = build_fx_url(entry["urlPath"], var, bbox)
            fallback_url = build_fileserver_url(entry["urlPath"])
            tasks.append((url, fallback_url, row, var))

    if args.dry_run:
        print(f"\n{len(tasks)} download tasks (dry-run, not fetched):", flush=True)
        for url, fallback_url, row, var in tasks:
            path = build_fx_dest(dest_root, row, var, domain_tag)
            print(f"  {path}\n    <- {url}\n    (fallback if NCSS fails: {fallback_url})")
        return

    print(f"\nDownloading {len(tasks)} files...", flush=True)
    dl_ok = dl_skip = dl_fail = dl_fallback = 0
    with concurrent.futures.ThreadPoolExecutor(max_workers=args.num_workers) as pool:
        futures = {
            pool.submit(download_with_fallback, url, fallback_url, dest_root, full_domain_root, row, var, domain_tag, args.retries): (row, var)
            for url, fallback_url, row, var in tasks
        }
        for i, fut in enumerate(concurrent.futures.as_completed(futures), 1):
            result = fut.result()
            print(f"[{i:>5}/{len(tasks)}] {result}", flush=True)
            if "fallback" in result and result.startswith("OK"):
                dl_fallback += 1
                dl_ok += 1
            elif result.startswith("OK"):
                dl_ok += 1
            elif result.startswith("SKIP"):
                dl_skip += 1
            else:
                dl_fail += 1

    print(f"\nDownload: OK={dl_ok} (of which full-domain fallback={dl_fallback}) SKIP={dl_skip} FAIL={dl_fail}", flush=True)


if __name__ == "__main__":
    main()
