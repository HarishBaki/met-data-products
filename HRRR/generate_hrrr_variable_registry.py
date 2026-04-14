# %%
#!/usr/bin/env python3
"""
Discover HRRR Zarr variables from the public hrrrzarr bucket and write a registry CSV.

This scans one sample cycle, typically:
  date=20250101, hour=00

for:
  family in {sfc, prs}
  run_type in {anl, fcst}

The output is source-derived rather than hand-maintained. It is intended to be
rich enough for downstream code to build source paths and inspect metadata.
"""

from __future__ import annotations

import argparse
import csv
import json
from pathlib import Path
from typing import Dict, Iterable, List, Optional

import s3fs
import zarr


DEFAULT_DATE = "20250101"
DEFAULT_HOUR = "00"
DEFAULT_FAMILIES = ("sfc", "prs")
DEFAULT_RUN_TYPES = ("anl", "fcst")
DEFAULT_OUTPUT = Path(__file__).with_name("hrrr_variable_registry.csv")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Generate a source-derived HRRR variable registry CSV.")
    parser.add_argument("--date", default=DEFAULT_DATE, help="Sample cycle date as YYYYMMDD")
    parser.add_argument("--hour", default=DEFAULT_HOUR, help="Sample cycle hour as HH")
    parser.add_argument("--families", nargs="+", default=list(DEFAULT_FAMILIES), help="Families to scan")
    parser.add_argument("--run-types", nargs="+", default=list(DEFAULT_RUN_TYPES), help="Run types to scan")
    parser.add_argument("--output", default=str(DEFAULT_OUTPUT), help="Output CSV path")
    return parser.parse_args()


def get_s3fs() -> s3fs.S3FileSystem:
    return s3fs.S3FileSystem(anon=True)


def safe_list(fs: s3fs.S3FileSystem, path: str) -> List[str]:
    try:
        return list(fs.ls(path, detail=False))
    except FileNotFoundError:
        return []


def basename(path: str) -> str:
    return path.rstrip("/").split("/")[-1]


def is_zarr_metadata_name(name: str) -> bool:
    return name.startswith(".z") or name in {".zgroup", ".zattrs", ".zmetadata", ".zarray"}


def filter_group_paths(paths: Iterable[str]) -> List[str]:
    return [path for path in paths if not is_zarr_metadata_name(basename(path))]


def sample_root(family: str, date: str, hour: str, run_type: str) -> str:
    return f"hrrrzarr/{family}/{date}/{date}_{hour}z_{run_type}.zarr"


def guess_target_var(attrs: Dict[str, object], source_var: str) -> str:
    for key in ("GRIB_cfVarName", "cfVarName", "cf_var_name", "short_name", "GRIB_shortName"):
        value = attrs.get(key)
        if value is not None and str(value).strip():
            return str(value).strip()
    return source_var


def maybe_get_attr(attrs: Dict[str, object], *keys: str) -> str:
    for key in keys:
        value = attrs.get(key)
        if value is not None and str(value).strip():
            return str(value).strip()
    return ""


def infer_dims(attrs: Dict[str, object], array_rank: int, run_type: str) -> List[str]:
    dims = attrs.get("_ARRAY_DIMENSIONS")
    if isinstance(dims, (list, tuple)) and dims:
        return [str(x) for x in dims]

    if run_type == "anl" and array_rank == 2:
        return ["y", "x"]
    if run_type == "fcst" and array_rank == 3:
        return ["forecast_step", "y", "x"]
    return [f"dim_{i}" for i in range(array_rank)]


def infer_include_in_cli(target_var: str, source_var: str, long_name: str) -> int:
    lowered = f"{target_var} {source_var} {long_name}".lower()
    if "orog" in lowered or "geopotential height" in lowered:
        return 0
    return 1


def open_array_attrs(fs: s3fs.S3FileSystem, array_path: str) -> Dict[str, object]:
    arr = zarr.open(fs.get_mapper(array_path), mode="r")
    attrs = dict(arr.attrs.asdict())
    attrs["_shape"] = list(arr.shape)
    attrs["_chunks"] = list(arr.chunks)
    attrs["_dtype"] = str(arr.dtype)
    return attrs


def build_record(
    family: str,
    run_type: str,
    level: str,
    source_var: str,
    root: str,
    attrs: Dict[str, object],
) -> Dict[str, str]:
    target_var = guess_target_var(attrs, source_var)
    long_name = maybe_get_attr(attrs, "long_name", "GRIB_name")
    units = maybe_get_attr(attrs, "units", "GRIB_units")
    standard_name = maybe_get_attr(attrs, "standard_name", "GRIB_cfName")
    short_name = maybe_get_attr(attrs, "GRIB_shortName", "short_name")
    cf_var_name = maybe_get_attr(attrs, "GRIB_cfVarName", "cfVarName", "cf_var_name")
    coords = maybe_get_attr(attrs, "coordinates")
    dims = infer_dims(attrs, len(attrs["_shape"]), run_type)
    shape = attrs["_shape"]
    chunks = attrs["_chunks"]
    array_group = f"{root}/{level}/{source_var}/{level}"
    array_path = f"{array_group}/{source_var}"
    include_in_cli = infer_include_in_cli(target_var, source_var, long_name)

    record = {
        "target_var": target_var,
        "long_name": long_name,
        "units": units,
        "family": family,
        "level": level,
        "source_var": source_var,
        "mode": run_type,
        "include_in_cli": str(include_in_cli),
        "notes": "",
        "run_type": run_type,
        "variable": source_var,
        "short_name": short_name,
        "cf_var_name": cf_var_name,
        "standard_name": standard_name,
        "coordinates": coords,
        "dims": ",".join(dims),
        "shape": json.dumps(shape, separators=(",", ":")),
        "chunks": json.dumps(chunks, separators=(",", ":")),
        "dtype": str(attrs["_dtype"]),
        "ndim": str(len(shape)),
        "group_path": f"{root}/{level}/{source_var}",
        "array_group": array_group,
        "array_path": array_path,
        "attrs_json": json.dumps(
            {k: v for k, v in attrs.items() if not k.startswith("_")},
            sort_keys=True,
            default=str,
        ),
    }
    return record


def discover_records(
    fs: s3fs.S3FileSystem,
    family: str,
    date: str,
    hour: str,
    run_type: str,
) -> Iterable[Dict[str, str]]:
    root = sample_root(family, date, hour, run_type)
    level_paths = filter_group_paths(safe_list(fs, root))
    for level_path in level_paths:
        level = basename(level_path)
        var_paths = filter_group_paths(safe_list(fs, level_path))
        for var_path in var_paths:
            source_var = basename(var_path)
            array_path = f"{root}/{level}/{source_var}/{level}/{source_var}"
            try:
                attrs = open_array_attrs(fs, array_path)
            except Exception as exc:
                yield {
                    "target_var": "",
                    "long_name": "",
                    "units": "",
                    "family": family,
                    "level": level,
                    "source_var": source_var,
                    "mode": run_type,
                    "include_in_cli": "0",
                    "notes": f"Failed to open array: {exc}",
                    "run_type": run_type,
                    "variable": source_var,
                    "short_name": "",
                    "cf_var_name": "",
                    "standard_name": "",
                    "coordinates": "",
                    "dims": "",
                    "shape": "",
                    "chunks": "",
                    "dtype": "",
                    "ndim": "",
                    "group_path": f"{root}/{level}/{source_var}",
                    "array_group": f"{root}/{level}/{source_var}/{level}",
                    "array_path": array_path,
                    "attrs_json": "",
                }
                continue

            yield build_record(family, run_type, level, source_var, root, attrs)


def sort_key(record: Dict[str, str]) -> tuple[str, str, str, str]:
    return (
        record["family"],
        record["run_type"],
        record["level"],
        record["source_var"],
    )


def write_csv(records: List[Dict[str, str]], out_path: Path) -> None:
    fieldnames = [
        "target_var",
        "long_name",
        "units",
        "family",
        "level",
        "source_var",
        "mode",
        "include_in_cli",
        "notes",
        "run_type",
        "variable",
        "short_name",
        "cf_var_name",
        "standard_name",
        "coordinates",
        "dims",
        "shape",
        "chunks",
        "dtype",
        "ndim",
        "group_path",
        "array_group",
        "array_path",
        "attrs_json",
    ]
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with out_path.open("w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for record in records:
            writer.writerow(record)


def main() -> None:
    args = parse_args()
    fs = get_s3fs()

    records: List[Dict[str, str]] = []
    for family in args.families:
        for run_type in args.run_types:
            records.extend(discover_records(fs, family, args.date, args.hour, run_type))

    records.sort(key=sort_key)
    out_path = Path(args.output)
    write_csv(records, out_path)
    print(f"Wrote {len(records)} rows to {out_path}")


if __name__ == "__main__":
    main()

# %%
