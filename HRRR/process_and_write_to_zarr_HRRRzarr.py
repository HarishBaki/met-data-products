# %%
#!/usr/bin/env python3
"""
Process public Utah HRRR Zarr into a region-cropped local Zarr store.

Design:
- One variable per invocation.
- Process a requested time range month-by-month.
- Source mode reads directly from the public `hrrrzarr` S3 bucket.
- Derived mode reads dependencies from the local Zarr store.
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

import numpy as np
import pandas as pd
import s3fs
import xarray as xr
import zarr
from joblib import Parallel, delayed

_BOOTSTRAP_ROOT = Path(__file__).resolve().parents[1]
if str(_BOOTSTRAP_ROOT) not in sys.path:
    sys.path.insert(0, str(_BOOTSTRAP_ROOT))

from data_utils.zarr_io import (
    convert_units_numpy,
    ensure_store,
    init_zarr,
    target_long_name,
    target_units,
)
from repo_utils import load_region_grid, load_region_vars


HRRRZARR_BUCKET = "hrrrzarr"
GRID_INDEX_STORE = f"s3://{HRRRZARR_BUCKET}/grid/HRRR_chunk_index.zarr"
# Region-derived when not explicitly overridden -- see resolve_region_crop().
DEFAULT_OUTPUT_ZARR = None
DEFAULT_OROG_PATH = None
REGISTRY_PATH = Path(__file__).with_name("hrrr_variable_registry.csv")
VAR_SPECS_PATH = Path(__file__).with_name("hrrr_var_specs.csv")

MANUAL_DEFAULTS = {
    "mode": "source",
    "var_name": "u10",
    "process_start": "2025-01-01T00",
    "process_end": "2025-01-31T23",
    "region": "New_York",
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
    "init_only": False,
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
        units="m s**-1",
        description="Derived from 10 m eastward and northward wind components as sqrt(u10^2 + v10^2).",
    ),
    "wdir10": DerivedSpec(
        var_name="wdir10",
        dependencies=("u10", "v10"),
        long_name="10 m wind direction",
        units="Degree true",
        description="Derived from 10 m wind components using meteorological convention: (270 - atan2(v10, u10) in degrees) mod 360.",
    ),
}


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


def convert_data_units(
    data: np.ndarray,
    var_name: str,
    src_units: Optional[str],
    output_units: Optional[str] = None,  # kept for call-site compatibility; ignored
) -> np.ndarray:
    """Convert numpy array to canonical units via zarr_io.convert_units_numpy."""
    return convert_units_numpy(data, var_name, src_units)


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


def resolve_region_crop(region: str) -> Tuple[dict, dict, slice, slice]:
    """Load configs/regions/{region}.yaml's grid.HRRR entry and turn it into
    (y_slice, x_slice) against HRRR's native grid, replacing the old NYS-only
    BBOX + compute_crop_slices() dynamic mask computation (which reproduced
    exactly the same 452x459 NYS crop, but only ever worked for that one
    hardcoded lat/lon box)."""
    region_grid = load_region_grid(region, "HRRR")
    region_vars = load_region_vars(region)
    assert region_grid["dims"] == ["y", "x"], region_grid["dims"]
    y_start = region_grid["crop"]["y_start"]
    x_start = region_grid["crop"]["x_start"]
    ny = region_grid["n0"]
    nx = region_grid["n1"]
    y_slice = slice(y_start, y_start + ny)
    x_slice = slice(x_start, x_start + nx)
    return region_grid, region_vars, y_slice, x_slice


def resolve_orog_path(explicit_orog_path: Optional[str], region: str, data_root: Optional[str], region_tag: str) -> str:
    """--orog-path if given explicitly; otherwise the region's persisted
    cropped_orography.nc, written by compute_and_write_region_crop.py
    --update-config (see that script's write_cropped_orography) -- replacing
    the old in-memory region_cropped_template_orog() fallback, which
    re-derived the same crop from hrrr_full_orography.nc on every run
    instead of reading what the crop tool already computed and persisted."""
    if explicit_orog_path is not None:
        return explicit_orog_path
    if not data_root:
        raise ValueError(f"configs/regions/{region}.yaml has no data_root set yet.")
    return f"{data_root}/HRRR_{region_tag}/cropped_orography.nc"


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


def ensure_initialized(
    zarr_store: str,
    full_dates: pd.DatetimeIndex,
    var_name: str,
    template_orog: xr.DataArray,
    time_chunk: int,
    y_chunk: int,
    x_chunk: int,
    region_tag: str = "region",
    synchronizer: Optional[zarr.ProcessSynchronizer] = None,
) -> None:
    # Delegates to the shared ensure_store()/init_zarr() (data_utils/zarr_io.py) -- same
    # store-exists/var-exists branching, same graph-construction-safe single-chunk +
    # explicit `encoding` write this file used to duplicate (and had independently
    # developed the same >2min-stall bug in, before being fixed here to match). HRRR's
    # own per-var registry attrs (family/target_var/level/run_type) and title ride along
    # via extra_var_attrs/global_title -- canonical long_name/units/_FillValue/
    # missing_value are computed identically either way (same target_long_name/
    # target_units calls), so final attrs are unchanged.
    var_spec, registry_entry = resolve_var_spec(var_name)
    extra_attrs = {
        "family": registry_entry.family,
        "target_var": registry_entry.target_var,
        "level": registry_entry.level,
        "run_type": registry_entry.mode,
        "source_attribute_note": "family, target_var, level, and run_type correspond to source HRRR attributes",
    }

    store_existed = os.path.exists(zarr_store)
    var_existed = False
    if store_existed:
        ds_check = open_local_zarr_with_retry(zarr_store)
        try:
            same_len = ds_check.sizes.get("time", -1) == full_dates.size
            same_vals = np.array_equal(pd.to_datetime(ds_check.time.values), pd.to_datetime(full_dates.values))
            if not (same_len and same_vals):
                raise ValueError("Time coordinate mismatch. Rebuild HRRR Zarr store.")
            var_existed = var_name in ds_check.data_vars
        finally:
            ds_check.close()

    if not store_existed:
        print(f"[init] store missing; initializing {zarr_store}")
    elif not var_existed:
        print(f"[init] store exists; adding missing variable {var_name}")
    else:
        print(f"[init] store exists; initialization skipped for {zarr_store}")

    ensure_store(
        zarr_store, full_dates, var_name, lambda: template_orog,
        {"time": time_chunk, "y": y_chunk, "x": x_chunk},
        global_title=f"{region_tag} Cropped HRRR Dataset",
        extra_var_attrs=extra_attrs,
        synchronizer=synchronizer,
    )
    print(f"[var] exists; creation skipped for {var_name}" if var_existed else f"[var] created: {var_name}")


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


def derived_attrs(spec: DerivedSpec, source_attrs: Dict[str, str]) -> Dict[str, object]:
    return {
        "long_name": target_long_name(spec.var_name),
        "units": target_units(spec.var_name),
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


def init_derived_var(
    zarr_store: str,
    var_name: str,
    spec: DerivedSpec,
    ds_meta: xr.Dataset,
    template_orog: xr.DataArray,
    time_chunk: int,
    y_chunk: int,
    x_chunk: int,
    synchronizer: Optional[zarr.ProcessSynchronizer] = None,
) -> None:
    if var_name in ds_meta.data_vars:
        print(f"[var] exists; creation skipped for {var_name}")
        return

    # Delegates to the shared init_zarr() (data_utils/zarr_io.py) -- template_orog already
    # has the right (y,x) shape/coords (validated against ds_meta by the caller), and
    # ds_meta's own time axis is passed through as full_times so the new var's time
    # coordinate matches exactly. derived_attrs()'s long_name/units are computed via the
    # same target_long_name/target_units calls init_zarr() itself uses for the canonical
    # override, so final attrs are unchanged from this function's previous hand-rolled version.
    source_attrs = derive_source_attrs(ds_meta, spec.dependencies)
    full_times = pd.DatetimeIndex(ds_meta.time.values)
    print(f"[init] store exists; adding missing derived variable {var_name}")
    init_zarr(
        zarr_store, full_times, template_orog, var_name,
        {"time": time_chunk, "y": y_chunk, "x": x_chunk},
        mode="a", global_title=ds_meta.attrs.get("title", ""),
        extra_var_attrs=derived_attrs(spec, source_attrs),
        synchronizer=synchronizer,
    )
    print(f"[var] created: {var_name}")


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
    da_out.attrs = derived_attrs(spec, source_attrs)
    return da_out.assign_coords(latitude=template_orog.latitude, longitude=template_orog.longitude).to_dataset()


def write_derived_month(
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
            "long_name": target_long_name(var_name),
            "units": target_units(var_name),
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
    parser = argparse.ArgumentParser(description="Process Utah HRRR Zarr into a region-cropped Zarr store.")
    source_cli_vars = sorted(name for name, spec in VAR_SPECS.items() if spec.include_in_cli)
    derived_cli_vars = sorted(DERIVED_SPECS)
    cli_vars = sorted(set(source_cli_vars).union(derived_cli_vars))
    parser.add_argument("--mode", choices=("source", "derived"), default="source")
    parser.add_argument("--var-name", choices=cli_vars)
    parser.add_argument("--process-start", help="Inclusive start time, e.g. 2025-01-01T00")
    parser.add_argument("--process-end", help="Inclusive end time, e.g. 2025-01-31T23")
    parser.add_argument(
        "--region", type=str, default=None,
        help="Region config name under configs/regions/ (e.g. New_York, New_Mexico) -- "
             "supplies the crop (grid.HRRR) and, when --output-zarr/--orog-path are not "
             "given explicitly, the output location (data_root/region_tag) and template "
             "orography too. REQUIRED, no default -- an implicit New_York default "
             "previously risked silently running against the wrong region's data if "
             "omitted (MANUAL_DEFAULTS below is unaffected -- it's a separate, explicit, "
             "zero-CLI-args-only interactive/notebook shortcut, not a production path)."
    )
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
    parser.add_argument(
        "--init-only", action="store_true",
        help="Only ensure the output zarr store/variable skeleton exists, then exit -- no "
             "source read, no data written. Run this once per var, serially (one process at "
             "a time, not via sbatch), before fanning out the real per-var jobs in parallel "
             "-- otherwise multiple jobs can simultaneously see the store doesn't exist yet "
             "and race to create it (zarr.errors.GroupNotFoundError / 'Time coordinate "
             "mismatch' / stale-handle failures -- observed in production from this exact "
             "race for a brand-new region)."
    )
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

    if cleaned_argv and cleaned_argv[0] in {"source", "derived"}:
        cleaned_argv = ["--mode", cleaned_argv[0], *cleaned_argv[1:]]

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
    if args.region is None:
        missing.append("--region")
    if missing:
        parser.error(f"the following arguments are required: {', '.join(missing)}")
    if args.mode == "source" and args.var_name not in source_cli_vars:
        parser.error(f"--var-name '{args.var_name}' is not a source variable. Use --mode derived for derived variables.")
    if args.mode == "derived" and args.var_name not in derived_cli_vars:
        parser.error(f"--var-name '{args.var_name}' is not a derived variable. Use --mode source for source variables.")
    return args


def run_source_pipeline(args: argparse.Namespace) -> None:
    region_grid, region_vars, y_slice, x_slice = resolve_region_crop(args.region)

    grid = open_grid_index()
    lat, lon = cropped_latlon(grid, y_slice, x_slice)

    data_root = region_vars["data_root"]
    region_tag = region_vars["region_tag"]
    if args.output_zarr is None:
        if not data_root:
            raise ValueError(f"configs/regions/{args.region}.yaml has no data_root set yet.")
        args.output_zarr = f"{data_root}/HRRR_{region_tag}/HRRR_{region_tag}.zarr"

    orog_path = resolve_orog_path(args.orog_path, args.region, data_root, region_tag)
    if not os.path.exists(orog_path):
        raise FileNotFoundError(
            f"Orography template not found: {orog_path} -- run compute_and_write_region_crop.py "
            f"--product HRRR --grid-source HRRR/hrrr_full_orography.nc --mode reference ... "
            f"--region-config configs/regions/{args.region}.yaml --update-config first (it writes "
            f"this file as a side effect), or pass --orog-path explicitly."
        )
    template_orog = load_template_orography(orog_path)
    y_chunk = resolve_chunk_size(args.y_chunk, int(template_orog.sizes["y"]), "y")
    x_chunk = resolve_chunk_size(args.x_chunk, int(template_orog.sizes["x"]), "x")
    report_physical_chunk_size(args.var_name, template_orog, args.time_chunk, y_chunk, x_chunk)
    full_dates = pd.date_range(
        f"{args.full_start_year}-01-01T00",
        f"{args.full_end_year}-12-31T23",
        freq="1h",
    )

    zarr_sync = zarr.ProcessSynchronizer(f"{args.output_zarr}.sync")
    ensure_initialized(
        args.output_zarr,
        full_dates,
        args.var_name,
        template_orog,
        args.time_chunk,
        y_chunk,
        x_chunk,
        region_tag=region_tag,
        synchronizer=zarr_sync,
    )

    if args.init_only:
        print(f"[init-only] {args.output_zarr} ready for '{args.var_name}'")
        return

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


def run_derived_pipeline(args: argparse.Namespace) -> None:
    region_grid = load_region_grid(args.region, "HRRR")
    region_vars = load_region_vars(args.region)
    assert region_grid["dims"] == ["y", "x"], region_grid["dims"]

    data_root = region_vars["data_root"]
    region_tag = region_vars["region_tag"]
    if args.output_zarr is None:
        if not data_root:
            raise ValueError(f"configs/regions/{args.region}.yaml has no data_root set yet.")
        args.output_zarr = f"{data_root}/HRRR_{region_tag}/HRRR_{region_tag}.zarr"

    if not os.path.exists(args.output_zarr):
        raise FileNotFoundError(f"Target HRRR Zarr store not found: {args.output_zarr}")

    orog_path = resolve_orog_path(args.orog_path, args.region, data_root, region_tag)
    if not os.path.exists(orog_path):
        raise FileNotFoundError(
            f"Orography template not found: {orog_path} -- run compute_and_write_region_crop.py "
            f"--product HRRR --grid-source HRRR/hrrr_full_orography.nc --mode reference ... "
            f"--region-config configs/regions/{args.region}.yaml --update-config first (it writes "
            f"this file as a side effect), or pass --orog-path explicitly."
        )
    template_orog = load_template_orography(orog_path)

    spec = DERIVED_SPECS[args.var_name]
    zarr_sync = zarr.ProcessSynchronizer(f"{args.output_zarr}.sync")
    ds_meta = open_local_zarr_with_retry(args.output_zarr)
    ds_src: Optional[xr.Dataset] = None
    try:
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
        report_physical_chunk_size(args.var_name, template_orog, time_chunk, y_chunk, x_chunk)
        init_derived_var(
            args.output_zarr, args.var_name, spec, ds_meta, template_orog, time_chunk, y_chunk, x_chunk,
            synchronizer=zarr_sync,
        )

        if args.init_only:
            print(f"[init-only] {args.output_zarr} ready for '{args.var_name}'")
            return

        ds_src = open_local_zarr_with_retry(args.output_zarr)[list(spec.dependencies)]
        for month in iter_month_starts(args.process_start, args.process_end):
            month_begin, month_end = month_start_end(month)
            start = max(month_begin, args.process_start)
            end = min(month_end, args.process_end)
            if start > end:
                continue

            month_times = pd.date_range(start, end, freq="1h")
            if args.skip_complete_months and time_block_has_complete_data(args.output_zarr, month_times, args.var_name):
                print(f"[skip] {month.strftime('%Y-%m')} already complete for {args.var_name}")
                continue

            print(
                f"[month] {month.strftime('%Y-%m')} deriving {args.var_name}: "
                f"{start.strftime('%Y-%m-%dT%H')} to {end.strftime('%Y-%m-%dT%H')}"
            )
            ds_month = compute_derived_month(ds_src, spec, month_times, template_orog)
            write_derived_month(ds_month, args.output_zarr, full_dates, time_chunk, y_chunk, x_chunk)
    finally:
        if ds_src is not None:
            ds_src.close()
        ds_meta.close()

    if args.consolidate_metadata:
        zarr.consolidate_metadata(args.output_zarr)


def main() -> None:
    args = parse_args()
    args.process_start = pd.Timestamp(args.process_start)
    args.process_end = pd.Timestamp(args.process_end)
    if args.process_end < args.process_start:
        raise ValueError("--process-end must be greater than or equal to --process-start")

    if args.mode == "derived":
        run_derived_pipeline(args)
    else:
        run_source_pipeline(args)


# %%
if __name__ == "__main__":
    main()
