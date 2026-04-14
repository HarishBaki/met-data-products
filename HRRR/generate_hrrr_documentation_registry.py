#!/usr/bin/env python3
"""
Generate a documentation-derived HRRR variable registry CSV from the Mesowest
HRRR Zarr variable list.

This complements `generate_hrrr_variable_registry.py`, which only inspects Zarr
metadata and therefore cannot recover human-readable long names when the source
arrays only expose values like `level/source_var`.
"""

from __future__ import annotations

import argparse
import csv
import io
import urllib.request
from pathlib import Path
from typing import Dict, Iterable, List


DOC_CSV_URL = "https://mesowest.utah.edu/html/hrrr/zarr_documentation/output/zarr_variables.csv"
DEFAULT_OUTPUT = Path(__file__).with_name("hrrr_documentation_registry.csv")
DEFAULT_SOURCE_REGISTRY = Path(__file__).with_name("hrrr_variable_registry.csv")

DOC_COLUMNS = {
    "doc_id": "#",
    "documented_long_name": "Parameter Long Name",
    "documented_level": "Vertical Level",
    "source_var": "Parameter Short Name",
    "documented_units": "Units",
    "first_version_available": "1st Version Available",
    "analysis_or_forecast": "Analysis or Forecast",
    "documentation_notes": "Notes",
}

LEVEL_ALIASES = {
    "0.1_sigma_layer": "0.1_sigma_level",
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Generate a documentation-derived HRRR variable registry CSV.")
    parser.add_argument("--url", default=DOC_CSV_URL, help="Source documentation CSV URL")
    parser.add_argument("--output", default=str(DEFAULT_OUTPUT), help="Output CSV path")
    parser.add_argument(
        "--source-registry",
        default=str(DEFAULT_SOURCE_REGISTRY),
        help="Optional source-derived HRRR registry CSV used to annotate matches",
    )
    return parser.parse_args()


def clean_text(value: str) -> str:
    return " ".join((value or "").replace("\xa0", " ").strip().split())


def normalize_level(value: str) -> str:
    cleaned = clean_text(value)
    alias = LEVEL_ALIASES.get(cleaned, cleaned)
    return alias.replace("_Above_", "_above_").replace("_Ground", "_ground")


def normalize_mode(value: str) -> List[str]:
    cleaned = clean_text(value).lower()
    if cleaned == "both":
        return ["anl", "fcst"]
    if cleaned in {"anl", "fcst"}:
        return [cleaned]
    raise ValueError(f"Unsupported Analysis or Forecast value: {value!r}")


def fetch_documentation_rows(url: str) -> List[Dict[str, str]]:
    with urllib.request.urlopen(url) as response:
        payload = response.read().decode("utf-8-sig")
    return list(csv.DictReader(io.StringIO(payload)))


def load_source_registry(path: Path) -> Dict[tuple[str, str, str], List[Dict[str, str]]]:
    if not path.exists():
        return {}

    with path.open(newline="") as handle:
        rows = list(csv.DictReader(handle))

    index: Dict[tuple[str, str, str], List[Dict[str, str]]] = {}
    for row in rows:
        key = (
            normalize_level(row.get("level", "")),
            clean_text(row.get("source_var", "")),
            clean_text(row.get("run_type", "")).lower(),
        )
        index.setdefault(key, []).append(row)
    return index


def build_records(
    doc_rows: Iterable[Dict[str, str]],
    source_index: Dict[tuple[str, str, str], List[Dict[str, str]]],
) -> List[Dict[str, str]]:
    records: List[Dict[str, str]] = []
    for doc_row in doc_rows:
        raw_level = clean_text(doc_row[DOC_COLUMNS["documented_level"]])
        normalized_level = normalize_level(raw_level)
        source_var = clean_text(doc_row[DOC_COLUMNS["source_var"]])
        raw_mode = clean_text(doc_row[DOC_COLUMNS["analysis_or_forecast"]]).lower()

        for run_type in normalize_mode(raw_mode):
            matches = source_index.get((normalized_level, source_var, run_type), [])
            records.append(
                {
                    "doc_id": clean_text(doc_row[DOC_COLUMNS["doc_id"]]),
                    "documented_long_name": clean_text(doc_row[DOC_COLUMNS["documented_long_name"]]),
                    "documented_level": raw_level,
                    "normalized_level": normalized_level,
                    "level_alias_applied": "1" if raw_level != normalized_level else "0",
                    "source_var": source_var,
                    "documented_units": clean_text(doc_row[DOC_COLUMNS["documented_units"]]),
                    "first_version_available": clean_text(doc_row[DOC_COLUMNS["first_version_available"]]),
                    "analysis_or_forecast": raw_mode,
                    "run_type": run_type,
                    "documentation_notes": clean_text(doc_row[DOC_COLUMNS["documentation_notes"]]),
                    "matched_source_registry_rows": str(len(matches)),
                    "matched_families": ",".join(sorted({clean_text(row.get("family", "")) for row in matches if row.get("family")})),
                    "matched_registry_levels": ",".join(
                        sorted({clean_text(row.get("level", "")) for row in matches if row.get("level")})
                    ),
                    "matched_registry_long_names": " | ".join(
                        sorted({clean_text(row.get("long_name", "")) for row in matches if row.get("long_name")})
                    ),
                }
            )
    return records


def write_csv(records: List[Dict[str, str]], out_path: Path) -> None:
    fieldnames = [
        "doc_id",
        "documented_long_name",
        "documented_level",
        "normalized_level",
        "level_alias_applied",
        "source_var",
        "documented_units",
        "first_version_available",
        "analysis_or_forecast",
        "run_type",
        "documentation_notes",
        "matched_source_registry_rows",
        "matched_families",
        "matched_registry_levels",
        "matched_registry_long_names",
    ]
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with out_path.open("w", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        for record in records:
            writer.writerow(record)


def main() -> None:
    args = parse_args()
    out_path = Path(args.output)
    source_registry_path = Path(args.source_registry)

    doc_rows = fetch_documentation_rows(args.url)
    source_index = load_source_registry(source_registry_path)
    records = build_records(doc_rows, source_index)
    write_csv(records, out_path)
    print(f"Wrote {len(records)} rows to {out_path}")


if __name__ == "__main__":
    main()
