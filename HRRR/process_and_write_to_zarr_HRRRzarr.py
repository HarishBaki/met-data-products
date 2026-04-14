# %%
#!/usr/bin/env python3
"""
Process public Utah HRRR Zarr into an NYS-cropped local Zarr store.

Design:
- One variable per invocation.
- Process a requested time range month-by-month.
- Read directly from the public `hrrrzarr` S3 bucket.
- Write into a single local Zarr store with HRRR-style target names
  matching the existing GRIB-based pipeline.
"""

from __future__ import annotations

import argparse
import calendar
import errno
import os
import sys
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Iterable, Optional, Tuple

import dask.array as da
import numpy as np
import pandas as pd
import s3fs
import xarray as xr
import zarr
from joblib import Parallel, delayed


HRRRZARR_BUCKET = "hrrrzarr"
GRID_INDEX_STORE = f"s3://{HRRRZARR_BUCKET}/grid/HRRR_chunk_index.zarr"
DEFAULT_OUTPUT_ZARR = "/network/rit/lab/basulab/Projects/DFS/DATA/HRRR_NYS/HRRR_NYS.zarr"
DEFAULT_OROG_PATH = str(Path(__file__).with_name("hrrr_orography_cropped_nys.nc"))
REGISTRY_PATH = Path(__file__).with_name("hrrr_variable_registry.csv")
VAR_SPECS_PATH = Path(__file__).with_name("hrrr_var_specs.csv")

MANUAL_DEFAULTS = {
    "var_name": "u10",
    "process_start": "2025-01-01T00",
    "process_end": "2025-01-31T23",
    "output_zarr": DEFAULT_OUTPUT_ZARR,
    "orog_path": DEFAULT_OROG_PATH,
    "full_start_year": 2010,
    "full_end_year": 2040,
    "time_chunk": 24,
    "y_chunk": 128,
    "x_chunk": 144,
    "n_jobs": max(1, os.cpu_count() or 1),
    "skip_complete_months": True,
    "consolidate_metadata": True,
}

BBOX = {
    "lat_min": 38.0,
    "lat_max": 48.0,
    "lon_min": -82.0,
    "lon_max": -68.0,
}

UNIT_CONVERSIONS = {
    "tp": {
        "kg m-2": ("kg m-2", 1.0),
        "kg m**-2": ("kg m-2", 1.0),
        "mm": ("kg m-2", 1.0),
        "m": ("kg m-2", 1000.0),
    },
    "sp": {
        "Pa": ("Pa", 1.0),
        "hPa": ("Pa", 100.0),
    },
    "i10fg": {
        "m s-1": ("m s-1", 1.0),
        "m s**-1": ("m s-1", 1.0),
    },
    "u10": {
        "m s-1": ("m s-1", 1.0),
        "m s**-1": ("m s-1", 1.0),
    },
    "v10": {
        "m s-1": ("m s-1", 1.0),
        "m s**-1": ("m s-1", 1.0),
    },
    "t2m": {
        "K": ("K", 1.0),
    },
    "d2m": {
        "K": ("K", 1.0),
    },
    "sh2": {
        "kg kg-1": ("kg kg-1", 1.0),
    },
    "orog": {
        "m": ("m", 1.0),
    },
}

UNIT_ALIASES = {
    "kg m-2": "kg m-2",
    "kg m^-2": "kg m-2",
    "kg m**-2": "kg m-2",
    "mm": "mm",
    "m": "m",
    "pa": "Pa",
    "hpa": "hPa",
    "m s-1": "m s-1",
    "m s^-1": "m s-1",
    "m s**-1": "m s-1",
    "k": "K",
    "kg kg-1": "kg kg-1",
}


def _format_bytes(n_bytes: int) -> str:
    units = ["B", "KB", "MB", "GB", "TB"]
    value = float(n_bytes)
    unit = units[0]
    for candidate in units:
        unit = candidate
        if value < 1024 or candidate == units[-1]:
            break
        value /= 1024.0
    return f"{value:.2f} {unit}"


def _chunk_nbytes(shape: Tuple[int, ...], dtype: np.dtype) -> int:
    return int(np.prod(shape)) * np.dtype(dtype).itemsize


def resolve_chunk_size(requested: Optional[int], dim_size: int, dim_name: str) -> int:
    if requested is None:
        return dim_size
    if requested <= 0:
        raise ValueError(f"{dim_name}_chunk must be positive, got {requested}")
    return min(int(requested), int(dim_size))


@dataclass(frozen=True)
class RegistryKey:
    target_var: str
    family: str
    level: str
    run_type: str


@dataclass(frozen=True)
class RegistryEntry:
    target_var: str
    long_name: str
    units: str
    family: str
    level: str
    source_var: str
    mode: str
    include_in_cli: bool
    notes: str


@dataclass(frozen=True)
class VarSpec:
    var_name: str
    registry_key: RegistryKey
    output_long_name: str
    output_units: str
    source_units_override: Optional[str] = None
    include_in_cli: bool = True


def load_variable_registry(registry_path: Path) -> Dict[RegistryKey, RegistryEntry]:
    if not registry_path.exists():
        raise FileNotFoundError(
            f"Registry file not found: {registry_path}. "
            f"Generate it first with generate_hrrr_variable_registry.py."
        )
    required_cols = {
        "target_var",
        "long_name",
        "units",
        "family",
        "level",
        "source_var",
        "mode",
        "include_in_cli",
        "notes",
    }
    df = pd.read_csv(registry_path, dtype=str).fillna("")
    missing = required_cols.difference(df.columns)
    if missing:
        raise ValueError(f"Missing registry columns in {registry_path}: {sorted(missing)}")

    registry: Dict[RegistryKey, RegistryEntry] = {}
    for row in df.to_dict(orient="records"):
        target_var = row["target_var"].strip()
        if not target_var:
            raise ValueError(f"Blank target_var in {registry_path}")
        run_type = row.get("run_type", row["mode"]).strip()
        key = RegistryKey(
            target_var=target_var,
            family=row["family"].strip(),
            level=row["level"].strip(),
            run_type=run_type,
        )
        if key in registry:
            raise ValueError(f"Duplicate registry key {key} in {registry_path}")

        registry[key] = RegistryEntry(
            target_var=target_var,
            long_name=row["long_name"].strip(),
            units=row["units"].strip(),
            family=row["family"].strip(),
            level=row["level"].strip(),
            source_var=row["source_var"].strip(),
            mode=run_type,
            include_in_cli=row["include_in_cli"].strip().lower() in {"1", "true", "yes", "y"},
            notes=row["notes"].strip(),
        )

    return registry


VAR_REGISTRY = load_variable_registry(REGISTRY_PATH)


def load_var_specs(var_specs_path: Path, registry: Dict[RegistryKey, RegistryEntry]) -> Dict[str, VarSpec]:
    if not var_specs_path.exists():
        raise FileNotFoundError(f"Var spec file not found: {var_specs_path}")

    required_cols = {
        "var_name",
        "target_var",
        "family",
        "level",
        "run_type",
        "output_long_name",
        "output_units",
        "source_units_override",
        "include_in_cli",
    }
    df = pd.read_csv(var_specs_path, dtype=str).fillna("")
    missing = required_cols.difference(df.columns)
    if missing:
        raise ValueError(f"Missing var spec columns in {var_specs_path}: {sorted(missing)}")

    var_specs: Dict[str, VarSpec] = {}
    for row in df.to_dict(orient="records"):
        var_name = row["var_name"].strip()
        if not var_name:
            raise ValueError(f"Blank var_name in {var_specs_path}")
        if var_name in var_specs:
            raise ValueError(f"Duplicate var_name '{var_name}' in {var_specs_path}")

        registry_key = RegistryKey(
            target_var=row["target_var"].strip(),
            family=row["family"].strip(),
            level=row["level"].strip(),
            run_type=row["run_type"].strip(),
        )
        if registry_key not in registry:
            raise ValueError(
                f"Var spec '{var_name}' points to missing registry row {registry_key}. "
                f"Check {var_specs_path} against {REGISTRY_PATH}."
            )

        var_specs[var_name] = VarSpec(
            var_name=var_name,
            registry_key=registry_key,
            output_long_name=row["output_long_name"].strip(),
            output_units=row["output_units"].strip(),
            source_units_override=row["source_units_override"].strip() or None,
            include_in_cli=row["include_in_cli"].strip().lower() in {"1", "true", "yes", "y"},
        )

    return var_specs


VAR_SPECS = load_var_specs(VAR_SPECS_PATH, VAR_REGISTRY)


def normalize_units(units: Optional[str]) -> Optional[str]:
    if not units:
        return units
    key = " ".join(units.strip().lower().split())
    return UNIT_ALIASES.get(key, units)


def convert_data_units(
    data: np.ndarray,
    var_name: str,
    src_units: Optional[str],
    target_units: Optional[str],
) -> np.ndarray:
    src_norm = normalize_units(src_units)
    target_norm = normalize_units(target_units)
    if not src_norm or not target_norm or src_norm == target_norm:
        return data

    conv = UNIT_CONVERSIONS.get(var_name, {}).get(src_units) or UNIT_CONVERSIONS.get(var_name, {}).get(src_norm)
    if conv is None or normalize_units(conv[0]) != target_norm:
        raise ValueError(f"No unit conversion available for {var_name}: {src_units} -> {target_units}")

    out_units, factor = conv
    if normalize_units(out_units) != target_norm:
        raise ValueError(f"Conversion target mismatch for {var_name}: {out_units} != {target_units}")
    if factor == 1.0:
        return data
    return np.asarray(data * factor, dtype=np.float32)


def resolve_var_spec(var_name: str) -> Tuple[VarSpec, RegistryEntry]:
    try:
        var_spec = VAR_SPECS[var_name]
    except KeyError as exc:
        raise KeyError(f"Unsupported HRRR variable '{var_name}'. Add it to VAR_SPECS.") from exc
    try:
        registry_entry = VAR_REGISTRY[var_spec.registry_key]
    except KeyError as exc:
        raise KeyError(
            f"Registry row not found for '{var_name}' using key {var_spec.registry_key}. "
            f"Check {REGISTRY_PATH} and VAR_SPECS."
        ) from exc
    return var_spec, registry_entry


def ensure_parent_dir(path: str) -> None:
    Path(path).parent.mkdir(parents=True, exist_ok=True)


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


def get_s3fs() -> s3fs.S3FileSystem:
    return s3fs.S3FileSystem(anon=True)


def is_stale_file_handle_error(exc: BaseException) -> bool:
    cur: Optional[BaseException] = exc
    while cur is not None:
        if isinstance(cur, OSError):
            if cur.errno == errno.ESTALE:
                return True
            if "stale file handle" in str(cur).lower():
                return True
        cur = cur.__cause__ or cur.__context__
    return False


def open_local_zarr_with_retry(zarr_store: str, retries: int = 4, delay_seconds: float = 1.0) -> xr.Dataset:
    last_error: Optional[Exception] = None
    for attempt in range(1, retries + 1):
        try:
            return xr.open_zarr(zarr_store, consolidated=False)
        except Exception as exc:
            last_error = exc
            if not is_stale_file_handle_error(exc) or attempt == retries:
                raise
            print(
                f"[retry] stale file handle while opening {zarr_store} "
                f"(attempt {attempt}/{retries}); sleeping {delay_seconds * attempt:.1f}s"
            )
            time.sleep(delay_seconds * attempt)
    assert last_error is not None
    raise last_error


def open_grid_index() -> xr.Dataset:
    return xr.open_zarr(
        GRID_INDEX_STORE,
        consolidated=False,
        storage_options={"anon": True},
    )


def normalize_bbox_lon(lon: np.ndarray) -> Tuple[float, float]:
    lon_max = float(np.nanmax(lon))
    if lon_max > 180.0:
        return (
            BBOX["lon_min"] + 360.0 if BBOX["lon_min"] < 0 else BBOX["lon_min"],
            BBOX["lon_max"] + 360.0 if BBOX["lon_max"] < 0 else BBOX["lon_max"],
        )
    return BBOX["lon_min"], BBOX["lon_max"]


def compute_crop_slices(grid: xr.Dataset) -> Tuple[slice, slice]:
    lat = grid["latitude"].values
    lon = grid["longitude"].values
    lon_min, lon_max = normalize_bbox_lon(lon)
    mask = (
        (lat >= BBOX["lat_min"])
        & (lat <= BBOX["lat_max"])
        & (lon >= lon_min)
        & (lon <= lon_max)
    )
    ys, xs = np.where(mask)
    if ys.size == 0 or xs.size == 0:
        raise ValueError("No HRRR grid cells found inside the NYS bounding box.")
    return slice(int(ys.min()), int(ys.max()) + 1), slice(int(xs.min()), int(xs.max()) + 1)


def cropped_latlon(grid: xr.Dataset, y_slice: slice, x_slice: slice) -> Tuple[xr.DataArray, xr.DataArray]:
    lat = grid["latitude"].isel(y=y_slice, x=x_slice).rename({"y": "y", "x": "x"})
    lon = grid["longitude"].isel(y=y_slice, x=x_slice).rename({"y": "y", "x": "x"})
    lat.attrs = {"standard_name": "latitude", "units": "degrees_north"}
    lon.attrs = {"standard_name": "longitude", "units": "degrees_east"}
    return lat, lon


def build_array_path(source_spec: RegistryEntry, init_time: pd.Timestamp) -> str:
    date_str = init_time.strftime("%Y%m%d")
    hour_str = init_time.strftime("%H")
    run_kind = "anl" if source_spec.mode == "anl" else "fcst"
    return (
        f"{HRRRZARR_BUCKET}/{source_spec.family}/{date_str}/{date_str}_{hour_str}z_{run_kind}.zarr/"
        f"{source_spec.level}/{source_spec.source_var}/{source_spec.level}/{source_spec.source_var}"
    )


def read_cropped_array(
    fs: s3fs.S3FileSystem,
    source_spec: RegistryEntry,
    valid_time: pd.Timestamp,
    y_slice: slice,
    x_slice: slice,
) -> np.ndarray:
    if source_spec.mode == "anl":
        init_time = valid_time
        array_index = (slice(y_slice.start, y_slice.stop), slice(x_slice.start, x_slice.stop))
    elif source_spec.mode in {"fcst", "fcst_lead1"}:
        init_time = valid_time - pd.Timedelta(hours=1)
        array_index = (0, slice(y_slice.start, y_slice.stop), slice(x_slice.start, x_slice.stop))
    else:
        raise ValueError(f"Unsupported source mode: {source_spec.mode}")

    mapper = fs.get_mapper(build_array_path(source_spec, init_time))
    arr = zarr.open(mapper, mode="r")
    data = np.asarray(arr[array_index], dtype=np.float32)
    return data


def load_template_orography(orog_path: str) -> xr.DataArray:
    return xr.open_dataset(orog_path).orog.load()


def report_physical_chunk_size(
    var_name: str,
    template_orog: xr.DataArray,
    time_chunk: int,
    y_chunk: int,
    x_chunk: int,
    dtype: np.dtype = np.float32,
) -> None:
    chunk_shape = (
        int(time_chunk),
        int(y_chunk),
        int(x_chunk),
    )
    chunk_bytes = _chunk_nbytes(chunk_shape, dtype)
    print(
        f"[chunk] {var_name}: shape={chunk_shape}, dtype={np.dtype(dtype)}, size={_format_bytes(chunk_bytes)}"
    )


def init_zarr_store(
    zarr_store: str,
    dates: pd.DatetimeIndex,
    var_name: str,
    template_orog: xr.DataArray,
    time_chunk: int,
    y_chunk: int,
    x_chunk: int,
    mode: str,
    write_global_attrs: bool,
) -> None:
    shape = (len(dates),) + template_orog.shape
    data = da.full(shape, np.nan, chunks=(time_chunk, y_chunk, x_chunk), dtype="float32")
    base = xr.DataArray(
        data,
        dims=("time", "y", "x"),
        coords={
            "time": dates,
            "latitude": template_orog.latitude,
            "longitude": template_orog.longitude,
        },
        name=var_name,
    )
    ds_init = base.to_dataset()
    if write_global_attrs:
        ds_init.attrs = {
            "title": "NYS Cropped HRRR Dataset",
            "Conventions": "CF-1.8",
            "history": "Initialized empty Zarr store from Utah HRRR Zarr",
        }
    var_spec, registry_entry = resolve_var_spec(var_name)
    ds_init[var_name].attrs = {
        "long_name": var_spec.output_long_name,
        "units": var_spec.output_units,
        "family": registry_entry.family,
        "target_var": registry_entry.target_var,
        "level": registry_entry.level,
        "run_type": registry_entry.mode,
        "source_attribute_note": "family, target_var, level, and run_type correspond to source HRRR attributes",
        "_FillValue": np.nan,
        "missing_value": np.nan,
    }
    ensure_parent_dir(zarr_store)
    action = "creating store" if mode == "w" else "adding variable"
    print(f"[init] {action}: {zarr_store} ({var_name})")
    ds_init.to_zarr(zarr_store, mode=mode, compute=False, consolidated=False, zarr_format=2)


def ensure_initialized(
    zarr_store: str,
    full_dates: pd.DatetimeIndex,
    var_name: str,
    template_orog: xr.DataArray,
    time_chunk: int,
    y_chunk: int,
    x_chunk: int,
) -> None:
    if not os.path.exists(zarr_store):
        print(f"[init] store missing; initializing {zarr_store}")
        init_zarr_store(
            zarr_store,
            full_dates,
            var_name,
            template_orog,
            time_chunk,
            y_chunk,
            x_chunk,
            mode="w",
            write_global_attrs=True,
        )
        print(f"[var] created: {var_name}")
        return

    ds_meta = open_local_zarr_with_retry(zarr_store)
    try:
        same_len = ds_meta.sizes.get("time", -1) == full_dates.size
        same_vals = np.array_equal(pd.to_datetime(ds_meta.time.values), pd.to_datetime(full_dates.values))
        if not (same_len and same_vals):
            raise ValueError("Time coordinate mismatch. Rebuild HRRR Zarr store.")

        if var_name not in ds_meta.data_vars:
            print(f"[init] store exists; adding missing variable {var_name}")
            init_zarr_store(
                zarr_store,
                full_dates,
                var_name,
                template_orog,
                time_chunk,
                y_chunk,
                x_chunk,
                mode="a",
                write_global_attrs=False,
            )
            print(f"[var] created: {var_name}")
        else:
            print(f"[init] store exists; initialization skipped for {zarr_store}")
            print(f"[var] exists; creation skipped for {var_name}")
    finally:
        ds_meta.close()


def time_block_has_complete_data(zarr_store: str, block_times: pd.DatetimeIndex, var_name: str) -> bool:
    ds = open_local_zarr_with_retry(zarr_store)
    try:
        if var_name not in ds.data_vars:
            return False
        try:
            block = ds[var_name].sel(time=block_times)
        except KeyError:
            return False
        expected_shape = (len(block_times), ds.sizes["y"], ds.sizes["x"])
        if tuple(block.shape) != expected_shape:
            return False
        return bool(block.notnull().all().compute())
    finally:
        ds.close()


def fetch_one_timestamp(
    timestamp: pd.Timestamp,
    var_spec: VarSpec,
    source_spec: RegistryEntry,
    y_slice: slice,
    x_slice: slice,
) -> Tuple[pd.Timestamp, Optional[np.ndarray], Optional[str]]:
    fs = get_s3fs()
    try:
        data = read_cropped_array(fs, source_spec, timestamp, y_slice, x_slice)
        src_units = var_spec.source_units_override or source_spec.units
        data = convert_data_units(data, var_spec.var_name, src_units, var_spec.output_units)
        return timestamp, data, None
    except Exception as exc:
        return timestamp, None, str(exc)


def iter_time_blocks(times: pd.DatetimeIndex, block_size: int) -> Iterable[pd.DatetimeIndex]:
    if block_size <= 0:
        raise ValueError(f"block_size must be positive, got {block_size}")
    for start_idx in range(0, len(times), block_size):
        yield times[start_idx : start_idx + block_size]


def build_time_block_dataset(
    block_times: pd.DatetimeIndex,
    var_name: str,
    y_slice: slice,
    x_slice: slice,
    lat: xr.DataArray,
    lon: xr.DataArray,
    n_jobs: int,
) -> xr.Dataset:
    var_spec, source_spec = resolve_var_spec(var_name)
    crop_shape = (y_slice.stop - y_slice.start, x_slice.stop - x_slice.start)
    results = Parallel(n_jobs=n_jobs, backend="threading", verbose=0)(
        delayed(fetch_one_timestamp)(ts, var_spec, source_spec, y_slice, x_slice) for ts in block_times
    )
    data_by_time: Dict[pd.Timestamp, np.ndarray] = {}
    for ts, arr, error in results:
        if error is not None:
            print(f"[warn] Missing {var_name} at {ts}: {error}")
            arr = np.full(crop_shape, np.nan, dtype=np.float32)
        data_by_time[ts] = arr
    stacked = np.stack([data_by_time[ts] for ts in block_times], axis=0).astype(np.float32, copy=False)
    da_out = xr.DataArray(
        stacked,
        dims=("time", "y", "x"),
        coords={"time": block_times, "latitude": lat, "longitude": lon},
        name=var_name,
        attrs={
            "long_name": var_spec.output_long_name,
            "units": var_spec.output_units,
            "family": source_spec.family,
            "target_var": source_spec.target_var,
            "level": source_spec.level,
            "run_type": source_spec.mode,
            "source_attribute_note": "family, target_var, level, and run_type correspond to source HRRR attributes",
            "_FillValue": np.nan,
            "missing_value": np.nan,
        },
    )
    return da_out.to_dataset()


def write_time_block(
    ds_block: xr.Dataset,
    zarr_store: str,
    full_dates: pd.DatetimeIndex,
    time_chunk: int,
    y_chunk: int,
    x_chunk: int,
) -> None:
    ds_block = ds_block.chunk(
        {
            "time": min(time_chunk, ds_block.sizes["time"]),
            "y": min(y_chunk, ds_block.sizes["y"]),
            "x": min(x_chunk, ds_block.sizes["x"]),
        }
    )
    start_idx = int(np.searchsorted(full_dates.values, ds_block.time.values[0]))
    end_idx = start_idx + ds_block.sizes["time"]
    region = {"time": slice(start_idx, end_idx)}
    write_ds = ds_block.drop_vars(["latitude", "longitude"], errors="ignore").assign_coords(time=ds_block.time)
    for name in write_ds.data_vars:
        write_ds[name].attrs.pop("_FillValue", None)
        write_ds[name].attrs.pop("missing_value", None)
    write_ds.to_zarr(zarr_store, mode="a", region=region, consolidated=False, zarr_format=2)
    print(
        f"[write] {pd.Timestamp(ds_block.time.values[0]).strftime('%Y-%m-%dT%H')} "
        f"to {pd.Timestamp(ds_block.time.values[-1]).strftime('%Y-%m-%dT%H')} -> region {region}"
    )


def process_one_month(
    month_start: pd.Timestamp,
    args: argparse.Namespace,
    full_dates: pd.DatetimeIndex,
    lat: xr.DataArray,
    lon: xr.DataArray,
    y_slice: slice,
    x_slice: slice,
    time_chunk: int,
    y_chunk: int,
    x_chunk: int,
) -> None:
    month_begin, month_end = month_start_end(month_start)
    start = max(month_begin, args.process_start)
    end = min(month_end, args.process_end)
    if start > end:
        return

    month_times = pd.date_range(start, end, freq="1h")

    print(
        f"[month] {month_start.strftime('%Y-%m')} processing: "
        f"{start.strftime('%Y-%m-%dT%H')} to {end.strftime('%Y-%m-%dT%H')}"
    )

    processed_blocks = 0
    skipped_blocks = 0
    for block_times in iter_time_blocks(month_times, time_chunk):
        block_start = pd.Timestamp(block_times[0]).strftime('%Y-%m-%dT%H')
        block_end = pd.Timestamp(block_times[-1]).strftime('%Y-%m-%dT%H')
        if args.skip_complete_months and time_block_has_complete_data(args.output_zarr, block_times, args.var_name):
            print(f"[day] skipped {args.var_name}: {block_start} to {block_end} already present")
            skipped_blocks += 1
            continue
        print(
            f"[day] building {args.var_name}: "
            f"{block_start} to {block_end}"
        )
        ds_block = build_time_block_dataset(block_times, args.var_name, y_slice, x_slice, lat, lon, args.n_jobs)
        write_time_block(ds_block, args.output_zarr, full_dates, time_chunk, y_chunk, x_chunk)
        processed_blocks += 1

    if processed_blocks == 0 and skipped_blocks > 0:
        print(f"[month] {month_start.strftime('%Y-%m')} skipped: all blocks already present for {args.var_name}")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Process Utah HRRR Zarr into a NYS-cropped Zarr store.")
    cli_vars = sorted(name for name, spec in VAR_SPECS.items() if spec.include_in_cli)
    parser.add_argument("--var-name", choices=cli_vars)
    parser.add_argument("--process-start", help="Inclusive start time, e.g. 2025-01-01T00")
    parser.add_argument("--process-end", help="Inclusive end time, e.g. 2025-01-31T23")
    parser.add_argument("--output-zarr", default=DEFAULT_OUTPUT_ZARR)
    parser.add_argument("--orog-path", default=DEFAULT_OROG_PATH)
    parser.add_argument("--full-start-year", type=int, default=2010)
    parser.add_argument("--full-end-year", type=int, default=2040)
    parser.add_argument("--time-chunk", type=int, default=24)
    parser.add_argument("--y-chunk", type=int, default=None)
    parser.add_argument("--x-chunk", type=int, default=None)
    parser.add_argument("--n-jobs", type=int, default=max(1, os.cpu_count() or 1))
    parser.add_argument("--skip-complete-months", action="store_true")
    parser.add_argument("--consolidate-metadata", action="store_true")
    raw_argv = sys.argv[1:]
    cleaned_argv = []
    i = 0
    while i < len(raw_argv):
        token = raw_argv[i]
        if token in ("-f", "--f"):
            i += 2
            continue
        cleaned_argv.append(token)
        i += 1

    args = parser.parse_args(cleaned_argv)
    required = (args.var_name, args.process_start, args.process_end)
    if not cleaned_argv and any(value is None for value in required):
        print("[args] No CLI HRRR args provided; using MANUAL_DEFAULTS.")
        return argparse.Namespace(**MANUAL_DEFAULTS)

    missing = []
    if args.var_name is None:
        missing.append("--var-name")
    if args.process_start is None:
        missing.append("--process-start")
    if args.process_end is None:
        missing.append("--process-end")
    if missing:
        parser.error(f"the following arguments are required: {', '.join(missing)}")
    return args


def main() -> None:
    args = parse_args()
    args.process_start = pd.Timestamp(args.process_start)
    args.process_end = pd.Timestamp(args.process_end)
    if args.process_end < args.process_start:
        raise ValueError("--process-end must be greater than or equal to --process-start")

    grid = open_grid_index()
    y_slice, x_slice = compute_crop_slices(grid)
    lat, lon = cropped_latlon(grid, y_slice, x_slice)

    if not os.path.exists(args.orog_path):
        raise FileNotFoundError(f"Orography template not found: {args.orog_path}")

    template_orog = load_template_orography(args.orog_path)
    y_chunk = resolve_chunk_size(args.y_chunk, int(template_orog.sizes["y"]), "y")
    x_chunk = resolve_chunk_size(args.x_chunk, int(template_orog.sizes["x"]), "x")
    report_physical_chunk_size(args.var_name, template_orog, args.time_chunk, y_chunk, x_chunk)
    full_dates = pd.date_range(
        f"{args.full_start_year}-01-01T00",
        f"{args.full_end_year}-12-31T23",
        freq="1h",
    )

    ensure_initialized(
        args.output_zarr,
        full_dates,
        args.var_name,
        template_orog,
        args.time_chunk,
        y_chunk,
        x_chunk,
    )

    for month in iter_month_starts(args.process_start, args.process_end):
        process_one_month(
            month,
            args,
            full_dates,
            lat,
            lon,
            y_slice,
            x_slice,
            args.time_chunk,
            y_chunk,
            x_chunk,
        )

    if args.consolidate_metadata:
        zarr.consolidate_metadata(args.output_zarr)


# %%
if __name__ == "__main__":
    main()
