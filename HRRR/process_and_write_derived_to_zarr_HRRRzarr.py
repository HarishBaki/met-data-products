#!/usr/bin/env python3
"""
Compute derived HRRR variables from the local HRRR NYS Zarr store.

Design:
- One derived variable per invocation.
- Reads from the already-created local HRRR Zarr store.
- Writes derived variables back into the same Zarr store.

Current derived variables:
- si10: 10 m wind speed, derived from u10 and v10
- wdir10: 10 m wind direction, derived from u10 and v10
"""

from __future__ import annotations

import argparse
import calendar
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Iterable, Tuple

import dask.array as da
import numpy as np
import pandas as pd
import xarray as xr
import zarr


DEFAULT_OUTPUT_ZARR = "/network/rit/lab/basulab/Projects/DFS/DATA/HRRR_NYS/HRRR_NYS.zarr"
DEFAULT_OROG_PATH = str(Path(__file__).with_name("hrrr_orography_cropped_nys.nc"))


@dataclass(frozen=True)
class DerivedSpec:
    var_name: str
    dependencies: Tuple[str, ...]
    long_name: str
    units: str
    description: str


DERIVED_SPECS: Dict[str, DerivedSpec] = {
    "si10": DerivedSpec(
        var_name="si10",
        dependencies=("u10", "v10"),
        long_name="10 m wind speed",
        units="m s-1",
        description="Derived from 10 m eastward and northward wind components as sqrt(u10^2 + v10^2).",
    ),
    "wdir10": DerivedSpec(
        var_name="wdir10",
        dependencies=("u10", "v10"),
        long_name="10 m wind direction",
        units="degree",
        description="Derived from 10 m wind components using meteorological convention: (270 - atan2(v10, u10) in degrees) mod 360.",
    ),
}


def month_start_end(ts: pd.Timestamp) -> Tuple[pd.Timestamp, pd.Timestamp]:
    start = pd.Timestamp(year=ts.year, month=ts.month, day=1, hour=0)
    end_day = calendar.monthrange(ts.year, ts.month)[1]
    end = pd.Timestamp(year=ts.year, month=ts.month, day=end_day, hour=23)
    return start, end


def iter_month_starts(start: pd.Timestamp, end: pd.Timestamp) -> Iterable[pd.Timestamp]:
    cur = pd.Timestamp(year=start.year, month=start.month, day=1, hour=0)
    final = pd.Timestamp(year=end.year, month=end.month, day=1, hour=0)
    while cur <= final:
        yield cur
        cur = cur + pd.offsets.MonthBegin(1)


def load_orography_template(orog_path: str) -> xr.DataArray:
    return xr.open_dataset(orog_path).orog.load()


def resolve_chunk_size(requested: int | None, dim_size: int, dim_name: str) -> int:
    if requested is None:
        return dim_size
    if requested <= 0:
        raise ValueError(f"{dim_name}_chunk must be positive, got {requested}")
    return min(int(requested), int(dim_size))


def infer_existing_chunk_size(ds: xr.Dataset, var_name: str, dim_name: str, fallback: int) -> int:
    var = ds[var_name]
    try:
        dim_index = var.get_axis_num(dim_name)
    except ValueError:
        return fallback

    data_chunks = getattr(var.data, "chunks", None)
    if data_chunks and dim_index < len(data_chunks) and data_chunks[dim_index]:
        return int(data_chunks[dim_index][0])

    encoding_chunks = var.encoding.get("chunks")
    if encoding_chunks and dim_index < len(encoding_chunks):
        return int(encoding_chunks[dim_index])

    return fallback


def derive_source_attrs(ds: xr.Dataset, dependencies: Tuple[str, ...]) -> Dict[str, str]:
    keys = ("family", "target_var", "level", "run_type")
    resolved: Dict[str, str] = {}
    for key in keys:
        values = []
        for dep in dependencies:
            value = str(ds[dep].attrs.get(key, "")).strip()
            if value:
                values.append(value)
        if not values:
            continue
        unique_values = sorted(set(values))
        if len(unique_values) != 1:
            if key == "target_var":
                resolved[key] = ", ".join(unique_values)
                continue
            raise ValueError(
                f"Inconsistent source attribute '{key}' across dependencies {dependencies}: {unique_values}"
            )
        resolved[key] = unique_values[0]
    return resolved


def init_derived_var(
    zarr_store: str,
    var_name: str,
    spec: DerivedSpec,
    ds_meta: xr.Dataset,
    template_orog: xr.DataArray,
    time_chunk: int,
    y_chunk: int,
    x_chunk: int,
) -> None:
    if var_name in ds_meta.data_vars:
        return

    y_size = ds_meta.sizes["y"]
    x_size = ds_meta.sizes["x"]
    data = da.full(
        (ds_meta.sizes["time"], y_size, x_size),
        np.nan,
        chunks=(time_chunk, y_chunk, x_chunk),
        dtype="float32",
    )
    ds_init = xr.Dataset(
        {
            var_name: xr.DataArray(
                data,
                dims=("time", "y", "x"),
                coords={
                    "time": ds_meta.time,
                    "latitude": template_orog.latitude,
                    "longitude": template_orog.longitude,
                },
            )
        }
    )
    source_attrs = derive_source_attrs(ds_meta, spec.dependencies)
    ds_init[var_name].attrs = {
        "long_name": spec.long_name,
        "units": spec.units,
        "description": spec.description,
        "dependencies": ", ".join(spec.dependencies),
        "family": source_attrs.get("family", ""),
        "target_var": source_attrs.get("target_var", ""),
        "level": source_attrs.get("level", ""),
        "run_type": source_attrs.get("run_type", ""),
        "source_attribute_note": (
            "family, level, and run_type correspond to shared source metadata on the dependency variables; "
            "target_var lists the source target vars when dependencies differ"
        ),
        "_FillValue": np.nan,
        "missing_value": np.nan,
    }
    ds_init.to_zarr(zarr_store, mode="a", compute=False, consolidated=False, zarr_format=2)


def month_has_complete_data(zarr_store: str, month_times: pd.DatetimeIndex, var_name: str) -> bool:
    ds = xr.open_zarr(zarr_store, consolidated=False)
    if var_name not in ds.data_vars:
        return False
    try:
        block = ds[var_name].sel(time=month_times)
    except KeyError:
        return False
    expected_shape = (len(month_times), ds.sizes["y"], ds.sizes["x"])
    if tuple(block.shape) != expected_shape:
        return False
    return bool(block.notnull().all().compute())


def compute_derived_month(
    ds_src: xr.Dataset,
    spec: DerivedSpec,
    month_times: pd.DatetimeIndex,
    template_orog: xr.DataArray,
) -> xr.Dataset:
    sub = ds_src[list(spec.dependencies)].sel(time=month_times)
    source_attrs = derive_source_attrs(ds_src, spec.dependencies)

    if spec.var_name == "si10":
        da_out = np.sqrt(sub["u10"] ** 2 + sub["v10"] ** 2)
    elif spec.var_name == "wdir10":
        da_out = ((270 - np.rad2deg(np.arctan2(sub["v10"], sub["u10"]))) % 360).where(
            sub["u10"].notnull() & sub["v10"].notnull()
        )
    else:
        raise ValueError(f"Unsupported derived variable: {spec.var_name}")

    da_out = da_out.astype(np.float32).rename(spec.var_name)
    da_out.attrs = {
        "long_name": spec.long_name,
        "units": spec.units,
        "description": spec.description,
        "dependencies": ", ".join(spec.dependencies),
        "family": source_attrs.get("family", ""),
        "target_var": source_attrs.get("target_var", ""),
        "level": source_attrs.get("level", ""),
        "run_type": source_attrs.get("run_type", ""),
        "source_attribute_note": (
            "family, level, and run_type correspond to shared source metadata on the dependency variables; "
            "target_var lists the source target vars when dependencies differ"
        ),
        "_FillValue": np.nan,
        "missing_value": np.nan,
    }
    return da_out.assign_coords(latitude=template_orog.latitude, longitude=template_orog.longitude).to_dataset()


def write_month(
    ds_month: xr.Dataset,
    zarr_store: str,
    full_dates: pd.DatetimeIndex,
    time_chunk: int,
    y_chunk: int,
    x_chunk: int,
) -> None:
    ds_month = ds_month.chunk(
        {
            "time": min(time_chunk, ds_month.sizes["time"]),
            "y": min(y_chunk, ds_month.sizes["y"]),
            "x": min(x_chunk, ds_month.sizes["x"]),
        }
    )
    start_idx = int(np.searchsorted(full_dates.values, ds_month.time.values[0]))
    end_idx = start_idx + ds_month.sizes["time"]
    region = {"time": slice(start_idx, end_idx)}
    write_ds = ds_month.drop_vars(["latitude", "longitude"], errors="ignore").assign_coords(time=ds_month.time)
    for name in write_ds.data_vars:
        write_ds[name].attrs.pop("_FillValue", None)
        write_ds[name].attrs.pop("missing_value", None)
    write_ds.to_zarr(zarr_store, mode="a", region=region, consolidated=False, zarr_format=2)
    print(f"[write] {pd.Timestamp(ds_month.time.values[0]).strftime('%Y-%m')} -> region {region}")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Compute derived HRRR variables into the local NYS Zarr store.")
    parser.add_argument("--var-name", required=True, choices=sorted(DERIVED_SPECS))
    parser.add_argument("--process-start", required=True, help="Inclusive start time, e.g. 2025-01-01T00")
    parser.add_argument("--process-end", required=True, help="Inclusive end time, e.g. 2025-01-31T23")
    parser.add_argument("--output-zarr", default=DEFAULT_OUTPUT_ZARR)
    parser.add_argument("--orog-path", default=DEFAULT_OROG_PATH)
    parser.add_argument("--time-chunk", type=int, default=24)
    parser.add_argument("--y-chunk", type=int, default=None)
    parser.add_argument("--x-chunk", type=int, default=None)
    parser.add_argument("--skip-complete-months", action="store_true")
    parser.add_argument("--consolidate-metadata", action="store_true")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    args.process_start = pd.Timestamp(args.process_start)
    args.process_end = pd.Timestamp(args.process_end)
    if args.process_end < args.process_start:
        raise ValueError("--process-end must be greater than or equal to --process-start")
    if not os.path.exists(args.output_zarr):
        raise FileNotFoundError(f"Target HRRR Zarr store not found: {args.output_zarr}")
    if not os.path.exists(args.orog_path):
        raise FileNotFoundError(f"Orography template not found: {args.orog_path}")

    spec = DERIVED_SPECS[args.var_name]
    template_orog = load_orography_template(args.orog_path)
    ds_meta = xr.open_zarr(args.output_zarr, consolidated=False)
    if template_orog.sizes["y"] != ds_meta.sizes["y"] or template_orog.sizes["x"] != ds_meta.sizes["x"]:
        raise ValueError(
            f"Orography template shape {dict(template_orog.sizes)} does not match target store "
            f"spatial shape y={ds_meta.sizes['y']}, x={ds_meta.sizes['x']}"
        )
    missing_deps = [name for name in spec.dependencies if name not in ds_meta.data_vars]
    if missing_deps:
        raise KeyError(f"Missing dependency variables in {args.output_zarr}: {missing_deps}")

    ref_var = spec.dependencies[0]
    time_chunk = resolve_chunk_size(
        args.time_chunk,
        infer_existing_chunk_size(ds_meta, ref_var, "time", int(ds_meta.sizes["time"])),
        "time",
    )
    y_chunk = resolve_chunk_size(
        args.y_chunk,
        infer_existing_chunk_size(ds_meta, ref_var, "y", int(ds_meta.sizes["y"])),
        "y",
    )
    x_chunk = resolve_chunk_size(
        args.x_chunk,
        infer_existing_chunk_size(ds_meta, ref_var, "x", int(ds_meta.sizes["x"])),
        "x",
    )

    full_dates = pd.DatetimeIndex(ds_meta.time.values)
    init_derived_var(args.output_zarr, args.var_name, spec, ds_meta, template_orog, time_chunk, y_chunk, x_chunk)

    ds_src = xr.open_zarr(args.output_zarr, consolidated=False)[list(spec.dependencies)]
    for month in iter_month_starts(args.process_start, args.process_end):
        month_begin, month_end = month_start_end(month)
        start = max(month_begin, args.process_start)
        end = min(month_end, args.process_end)
        if start > end:
            continue

        month_times = pd.date_range(start, end, freq="1h")
        if args.skip_complete_months and month_has_complete_data(args.output_zarr, month_times, args.var_name):
            print(f"[skip] {month.strftime('%Y-%m')} already complete for {args.var_name}")
            continue

        ds_month = compute_derived_month(ds_src, spec, month_times, template_orog)
        write_month(ds_month, args.output_zarr, full_dates, time_chunk, y_chunk, x_chunk)

    if args.consolidate_metadata:
        zarr.consolidate_metadata(args.output_zarr)


if __name__ == "__main__":
    main()
