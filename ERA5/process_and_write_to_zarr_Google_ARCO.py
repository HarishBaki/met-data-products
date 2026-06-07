#!/usr/bin/env python3
"""
Process ERA5 from Google ARCO into grouped NYS Zarr.

Design:
- Single output Zarr root store.
- Three groups:
  - sl: surface variables (time, latitude, longitude)
  - pl: pressure-level variables (time, level, latitude, longitude)
  - ml: model-level variables (time, model_level, latitude, longitude)
- One variable per run, configurable date range and selected levels.
- Internal daily parallelism writes non-overlapping regions.
"""

from __future__ import annotations

import argparse
import csv
import os
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Optional, Sequence, Tuple

import dask.array as da
import numpy as np
import pandas as pd
import xarray as xr
import zarr
from joblib import Parallel, delayed

_BOOTSTRAP_ROOT = Path(__file__).resolve().parents[1]
if str(_BOOTSTRAP_ROOT) not in sys.path:
    sys.path.insert(0, str(_BOOTSTRAP_ROOT))

from data_utils.zarr_io import apply_var_attrs, target_long_name, target_units

ARCO_SURFACE_PRESSURE_STORE = "gs://gcp-public-data-arco-era5/ar/full_37-1h-0p25deg-chunk-1.zarr-v3"
ARCO_MODEL_LEVEL_STORE = "gs://gcp-public-data-arco-era5/ar/model-level-1h-0p25deg.zarr-v1"

DEFAULT_OUTPUT_ZARR = (
    "/network/rit/lab/basulab/Projects/DFS/DATA/ERA5_NYS/ERA5_analysis_ARCO_NYS.zarr"
)
DEFAULT_REGISTRY_FILE = str(Path(__file__).with_name("arco_variable_registry.csv"))

GROUP_SURFACE = "sl"
GROUP_PRESSURE = "pl"
GROUP_MODEL = "ml"
GROUP_CHOICES = (GROUP_SURFACE, GROUP_PRESSURE, GROUP_MODEL)

LAT_MIN = 38.0
LAT_MAX = 48.0
LON_MIN = -82.0
LON_MAX = -68.0

TIME_CHUNK = 24
LEVEL_CHUNK = 1

SURFACE_DERIVED = {
    "si10": ["u10", "v10"],
    "wdir10": ["u10", "v10"],
}

# Minimal fallback registry-like info used only when registry/source attrs are missing.
# Keep this focused on variables your pipeline actually uses.
# Units are now canonical via var_meta.yaml / zarr_io.apply_var_attrs.
# Only source_var and long_name are retained here as lookup fallbacks.
FALLBACK_VAR_INFO: Dict[str, Dict[str, str]] = {
    # surface
    "u10": {"source_var": "10m_u_component_of_wind", "long_name": "10 m eastward wind"},
    "v10": {"source_var": "10m_v_component_of_wind", "long_name": "10 m northward wind"},
    "t2m": {"source_var": "2m_temperature", "long_name": "2 m air temperature"},
    "d2m": {"source_var": "2m_dewpoint_temperature", "long_name": "2 m dew point temperature"},
    "sp": {"source_var": "surface_pressure", "long_name": "surface pressure"},
    "tp": {"source_var": "total_precipitation", "long_name": "total precipitation"},
    "i10fg": {"source_var": "instantaneous_10m_wind_gust", "long_name": "10 m wind gust"},
    "si10": {"long_name": "10 m wind speed"},
    "wdir10": {"long_name": "10 m wind direction"},
    # pressure/model common
    "u": {"source_var": "u_component_of_wind", "long_name": "U component of wind"},
    "v": {"source_var": "v_component_of_wind", "long_name": "V component of wind"},
    "t": {"source_var": "temperature", "long_name": "Temperature"},
    "q": {"source_var": "specific_humidity", "long_name": "Specific humidity"},
    "z": {"source_var": "geopotential", "long_name": "Geopotential"},
}


@dataclass(frozen=True)
class JobConfig:
    variable: str
    source_variable: str
    target_variable: str
    group: str
    source_store: str
    process_start: pd.Timestamp
    process_end: pd.Timestamp
    output_zarr: str
    sync_path: str
    token: str
    time_chunk: int
    level_chunk: int
    skip_complete_steps: bool
    level_dim_out: Optional[str]
    level_values_all: Optional[np.ndarray]
    level_values_write: Optional[np.ndarray]
    level_indices_write: Optional[np.ndarray]
    source_map: Dict[str, str]
    registry_long_name: Optional[str]
    registry_units: Optional[str]


@dataclass(frozen=True)
class RegistryEntry:
    var_name: str
    source_var: str
    group: str
    target_var: Optional[str]
    long_name: Optional[str]
    units: Optional[str]


def normalize_group_label(group: str) -> str:
    g = (group or "").strip().lower()
    if g == "sf":  # backward compatibility
        return GROUP_SURFACE
    return g


def parse_int_list(values: Optional[Sequence[str] | str]) -> List[int]:
    if not values:
        return []
    if isinstance(values, str):
        values = [values]
    out: List[int] = []
    for item in values:
        for part in str(item).split(","):
            p = part.strip()
            if not p:
                continue
            if p.lower() == "all":
                continue
            out.append(int(p))
    return out


def load_registry_entries(path: str) -> List[RegistryEntry]:
    p = Path(path)
    out: List[RegistryEntry] = []
    if not p.exists():
        return out

    with p.open("r", newline="") as f:
        reader = csv.DictReader(f)
        required = {"var_name", "source_var"}
        missing = required.difference(set(reader.fieldnames or []))
        if missing:
            raise ValueError(f"Registry file {p} missing required columns: {sorted(missing)}")
        for row in reader:
            var_name = (row.get("var_name") or "").strip()
            source_var = (row.get("source_var") or "").strip()
            if not var_name or not source_var:
                continue
            out.append(
                RegistryEntry(
                    var_name=var_name,
                    source_var=source_var,
                    group=(row.get("group") or "").strip(),
                    target_var=((row.get("target_var") or "").strip() or None),
                    long_name=((row.get("long_name") or "").strip() or None),
                    units=((row.get("units") or "").strip() or None),
                )
            )
    return out


def build_group_source_map(entries: Sequence[RegistryEntry], group: str) -> Dict[str, str]:
    group = normalize_group_label(group)
    out: Dict[str, str] = {}
    for e in entries:
        row_group = normalize_group_label(e.group)
        if row_group and row_group not in (group, "any", "all"):
            continue
        out[e.var_name] = e.source_var
    return out


def lookup_registry_entry(entries: Sequence[RegistryEntry], group: str, var_name: str) -> Optional[RegistryEntry]:
    group = normalize_group_label(group)
    matches = [
        e
        for e in entries
        if e.var_name == var_name
        and (not e.group or normalize_group_label(e.group) in (group, "any", "all"))
    ]
    if not matches:
        return None
    # If the registry has duplicates for (group,var_name), fail fast.
    unique_sources = sorted(set(m.source_var for m in matches))
    if len(unique_sources) > 1:
        raise ValueError(
            f"Ambiguous registry mapping for var_name='{var_name}', group='{group}': "
            f"multiple source_var values {unique_sources}"
        )
    return matches[0]


def resolve_source_var(var_name: str, explicit_source: Optional[str], source_map: Dict[str, str]) -> str:
    if explicit_source:
        return explicit_source
    if var_name in source_map:
        return source_map[var_name]
    fb = FALLBACK_VAR_INFO.get(var_name, {})
    if "source_var" in fb and fb["source_var"]:
        return fb["source_var"]
    return var_name


def day_start_end(ts: pd.Timestamp) -> Tuple[pd.Timestamp, pd.Timestamp]:
    start = pd.Timestamp(year=ts.year, month=ts.month, day=ts.day, hour=0)
    end = start + pd.Timedelta(days=1) - pd.Timedelta(hours=1)
    return start, end


def normalize_lon_bounds(ds: xr.Dataset) -> Tuple[float, float]:
    lon_max_ds = float(ds.longitude.max())
    if lon_max_ds > 180:
        return (
            LON_MIN + 360.0 if LON_MIN < 0 else LON_MIN,
            LON_MAX + 360.0 if LON_MAX < 0 else LON_MAX,
        )
    return LON_MIN, LON_MAX


def crop_nys(ds: xr.Dataset) -> xr.Dataset:
    lon_min, lon_max = normalize_lon_bounds(ds)
    lat_slice = slice(LAT_MAX, LAT_MIN) if ds.latitude[0] > ds.latitude[-1] else slice(LAT_MIN, LAT_MAX)
    lon_slice = slice(lon_min, lon_max) if ds.longitude[0] < ds.longitude[-1] else slice(lon_max, lon_min)
    return ds.sel(latitude=lat_slice, longitude=lon_slice)


def open_source(store: str, token: str, time_chunk: int) -> xr.Dataset:
    try:
        return xr.open_zarr(
            store,
            chunks={"time": time_chunk},
            consolidated=True,
            storage_options={"token": token},
        )
    except ImportError as exc:
        raise ImportError("Missing dependency for gs:// access. Install gcsfs in this environment.") from exc
    except Exception:
        return xr.open_zarr(
            store,
            chunks={"time": time_chunk},
            consolidated=False,
            storage_options={"token": token},
        )


def detect_level_dim(ds: xr.Dataset, group: str) -> str:
    if group == GROUP_PRESSURE:
        if "level" in ds.dims or "level" in ds.coords:
            return "level"
        raise KeyError("Pressure-level data expects a 'level' dimension.")
    for c in ("model_level", "hybrid", "level"):
        if c in ds.dims or c in ds.coords:
            return c
    raise KeyError("Could not detect model-level dimension (expected model_level/hybrid/level).")


def map_levels_to_indices(all_levels: np.ndarray, requested_levels: Sequence[int]) -> Tuple[np.ndarray, np.ndarray]:
    if len(requested_levels) == 0:
        idx = np.arange(len(all_levels), dtype=np.int64)
        return all_levels[idx], idx

    idxs: List[int] = []
    for lv in requested_levels:
        matches = np.where(np.isclose(all_levels.astype(float), float(lv)))[0]
        if len(matches) == 0:
            raise ValueError(f"Requested level {lv} not found in available levels.")
        idxs.append(int(matches[0]))

    idx_arr = np.array(sorted(set(idxs)), dtype=np.int64)
    return all_levels[idx_arr], idx_arr


def contiguous_blocks(indices: np.ndarray) -> List[Tuple[int, int, int, int]]:
    """Return blocks as (pos_start, pos_end, idx_start, idx_end_exclusive)."""
    if len(indices) == 0:
        return []
    blocks: List[Tuple[int, int, int, int]] = []
    pos_start = 0
    for pos in range(1, len(indices)):
        if indices[pos] != indices[pos - 1] + 1:
            blocks.append((pos_start, pos, int(indices[pos_start]), int(indices[pos - 1] + 1)))
            pos_start = pos
    blocks.append((pos_start, len(indices), int(indices[pos_start]), int(indices[-1] + 1)))
    return blocks


def format_level_summary(cfg: JobConfig) -> str:
    if cfg.group == GROUP_SURFACE or cfg.level_values_write is None:
        return ""
    level_values = [int(x) for x in np.asarray(cfg.level_values_write).tolist()]
    level_indices = [] if cfg.level_indices_write is None else [int(x) for x in np.asarray(cfg.level_indices_write).tolist()]
    return f" {cfg.level_dim_out}_values={level_values} {cfg.level_dim_out}_indices={level_indices}"


def full_time_axis(full_start_year: int, full_end_year: int) -> pd.DatetimeIndex:
    return pd.date_range(
        f"{full_start_year}-01-01T00:00:00",
        f"{full_end_year}-12-31T23:00:00",
        freq="1h",
    )


def month_region_indices(full_times: pd.DatetimeIndex, times: pd.DatetimeIndex) -> Tuple[int, int]:
    start_idx = int(np.searchsorted(full_times.values, times[0].to_datetime64()))
    end_idx = int(np.searchsorted(full_times.values, times[-1].to_datetime64(), side="right"))
    if end_idx <= start_idx:
        raise ValueError("Computed invalid time region for write.")
    return start_idx, end_idx


def build_day_dataset(cfg: JobConfig, day_ts: pd.Timestamp) -> Optional[xr.Dataset]:
    ds = open_source(cfg.source_store, cfg.token, cfg.time_chunk)
    start, end = day_start_end(day_ts)

    if end < cfg.process_start or start > cfg.process_end:
        return None
    start = max(start, cfg.process_start)
    end = min(end, cfg.process_end)

    if cfg.group == GROUP_SURFACE:
        if cfg.variable in SURFACE_DERIVED:
            read_vars = [
                cfg.source_map.get(dep, FALLBACK_VAR_INFO.get(dep, {}).get("source_var", dep))
                for dep in SURFACE_DERIVED[cfg.variable]
            ]
        else:
            read_vars = [cfg.source_variable]
        sub = ds[read_vars].sel(time=slice(start, end))
        if sub.sizes.get("time", 0) == 0:
            return None
        sub = crop_nys(sub)

        if cfg.variable == "si10":
            u_key = cfg.source_map.get("u10", FALLBACK_VAR_INFO["u10"]["source_var"])
            v_key = cfg.source_map.get("v10", FALLBACK_VAR_INFO["v10"]["source_var"])
            da_var = np.sqrt(sub[u_key] ** 2 + sub[v_key] ** 2)
        elif cfg.variable == "wdir10":
            u_key = cfg.source_map.get("u10", FALLBACK_VAR_INFO["u10"]["source_var"])
            v_key = cfg.source_map.get("v10", FALLBACK_VAR_INFO["v10"]["source_var"])
            da_var = ((270 - np.rad2deg(np.arctan2(sub[v_key], sub[u_key]))) % 360).where(
                (sub[u_key] != 0) | (sub[v_key] != 0), other=0
            )
        else:
            da_var = sub[cfg.source_variable]

        attrs = dict(da_var.attrs)
        attrs["target_group"] = cfg.group
        attrs["source_store"] = cfg.source_store
        attrs["source_variable"] = cfg.source_variable
        if cfg.registry_long_name and not attrs.get("long_name"):
            attrs["long_name"] = cfg.registry_long_name
        if cfg.registry_units and not attrs.get("units"):
            attrs["units"] = cfg.registry_units
        da_var.attrs = attrs

        da_var = da_var.transpose("time", "latitude", "longitude").astype(np.float32)
        da_var = da_var.rename(cfg.target_variable)
        out = xr.Dataset({cfg.target_variable: da_var})
        out = out.chunk({"time": cfg.time_chunk, "latitude": out.sizes["latitude"], "longitude": out.sizes["longitude"]})
        out = apply_var_attrs(out, cfg.target_variable)
        return out

    if cfg.group not in (GROUP_PRESSURE, GROUP_MODEL):
        raise ValueError(f"Unknown group: {cfg.group}")

    if cfg.level_values_write is None or cfg.level_dim_out is None:
        raise ValueError("Level configuration missing for pl/ml job.")

    source_var_ds = ds[[cfg.source_variable]]
    level_dim_src = detect_level_dim(source_var_ds, cfg.group)
    sub = source_var_ds.sel({level_dim_src: cfg.level_values_write, "time": slice(start, end)})
    if sub.sizes.get("time", 0) == 0:
        return None
    sub = crop_nys(sub)
    da_var = sub[cfg.source_variable]

    if level_dim_src != cfg.level_dim_out:
        da_var = da_var.rename({level_dim_src: cfg.level_dim_out})

    da_var = da_var.transpose("time", cfg.level_dim_out, "latitude", "longitude").astype(np.float32)
    da_var = da_var.rename(cfg.target_variable)

    attrs = dict(da_var.attrs)
    attrs["target_group"] = cfg.group
    attrs["source_store"] = cfg.source_store
    attrs["source_variable"] = cfg.source_variable
    if not attrs.get("long_name"):
        attrs["long_name"] = (
            cfg.registry_long_name
            or FALLBACK_VAR_INFO.get(cfg.variable, {}).get("long_name", "")
        )
    if not attrs.get("units"):
        attrs["units"] = (
            cfg.registry_units
            or FALLBACK_VAR_INFO.get(cfg.variable, {}).get("units", "")
        )
    attrs["selected_levels"] = [int(x) for x in np.array(cfg.level_values_write).tolist()]
    da_var.attrs = attrs

    out = xr.Dataset({cfg.target_variable: da_var})
    out = out.chunk(
        {
            "time": cfg.time_chunk,
            cfg.level_dim_out: min(cfg.level_chunk, out.sizes[cfg.level_dim_out]),
            "latitude": out.sizes["latitude"],
            "longitude": out.sizes["longitude"],
        }
    )
    out = apply_var_attrs(out, cfg.target_variable)
    return out


def find_template_step(cfg: JobConfig, steps: Sequence[pd.Timestamp]) -> xr.Dataset:
    for ts in steps:
        out = build_day_dataset(cfg, ts)
        if out is not None and out.sizes.get("time", 0) > 0:
            return out
    raise RuntimeError("Could not build a template step for initialization. Check var/level/date range.")


def ensure_group_and_variable(cfg: JobConfig, full_times: pd.DatetimeIndex, steps: Sequence[pd.Timestamp]) -> None:
    output = Path(cfg.output_zarr)
    create_group = False
    lat = None
    lon = None

    try:
        ds_g = xr.open_zarr(cfg.output_zarr, group=cfg.group, consolidated=False)
        existing_times = pd.DatetimeIndex(ds_g.time.values)
        if not np.array_equal(existing_times.values, full_times.values):
            raise ValueError(f"Time axis mismatch in existing group '{cfg.group}'.")

        lat = ds_g.latitude
        lon = ds_g.longitude

        if cfg.group == GROUP_SURFACE:
            if cfg.target_variable in ds_g.data_vars:
                return
        else:
            if cfg.level_dim_out is None or cfg.level_values_all is None:
                raise ValueError("Missing level config for pl/ml initialization.")
            if cfg.level_dim_out not in ds_g.coords:
                raise ValueError(f"Existing group '{cfg.group}' missing coord '{cfg.level_dim_out}'.")
            existing_levels = np.asarray(ds_g[cfg.level_dim_out].values)
            if not np.array_equal(existing_levels, cfg.level_values_all):
                raise ValueError(f"Level axis mismatch in existing group '{cfg.group}'.")
            if cfg.target_variable in ds_g.data_vars:
                return
        mode = "a"
    except Exception:
        print(f"[init] group '{cfg.group}' missing or incompatible; building template from source", flush=True)
        template = find_template_step(cfg, steps)
        lat = template.latitude
        lon = template.longitude
        mode = "a" if output.exists() else "w"
        create_group = True

    if cfg.group == GROUP_SURFACE:
        shape = (len(full_times), lat.size, lon.size)
        dims = ("time", "latitude", "longitude")
        chunks = (cfg.time_chunk, lat.size, lon.size)
    else:
        if cfg.level_dim_out is None or cfg.level_values_all is None:
            raise ValueError("Missing level config for pl/ml initialization.")
        shape = (len(full_times), len(cfg.level_values_all), lat.size, lon.size)
        dims = ("time", cfg.level_dim_out, "latitude", "longitude")
        chunks = (cfg.time_chunk, min(cfg.level_chunk, len(cfg.level_values_all)), lat.size, lon.size)

    data = da.full(shape, np.nan, chunks=chunks, dtype=np.float32)
    if create_group:
        coords = {
            "time": full_times,
            "latitude": lat,
            "longitude": lon,
        }
        if cfg.group != GROUP_SURFACE:
            coords[cfg.level_dim_out] = cfg.level_values_all
        ds_init = xr.Dataset(
            {cfg.target_variable: xr.DataArray(data, dims=dims)},
            coords=coords,
        )
    else:
        ds_init = xr.Dataset({cfg.target_variable: xr.DataArray(data, dims=dims)})

    ds_init[cfg.target_variable].attrs = {
        "long_name": target_long_name(cfg.target_variable),
        "units": target_units(cfg.target_variable),
        "_FillValue": np.nan,
        "missing_value": np.nan,
        "target_group": cfg.group,
        "source_store": cfg.source_store,
        "source_variable": cfg.source_variable,
    }

    sync = zarr.ProcessSynchronizer(cfg.sync_path)
    print(
        f"[init] writing metadata for group={cfg.group} var={cfg.target_variable} create_group={create_group}",
        flush=True,
    )
    ds_init.to_zarr(
        cfg.output_zarr,
        group=cfg.group,
        mode=mode,
        compute=False,
        consolidated=False,
        zarr_format=2,
        synchronizer=sync,
    )
    print(f"[init] metadata ready for group={cfg.group} var={cfg.target_variable}", flush=True)


def is_step_complete(cfg: JobConfig, full_times: pd.DatetimeIndex, step_ds: xr.Dataset) -> bool:
    ds_g = xr.open_zarr(cfg.output_zarr, group=cfg.group, consolidated=False)
    if cfg.target_variable not in ds_g.data_vars:
        return False

    times = pd.DatetimeIndex(step_ds.time.values)
    start_idx, end_idx = month_region_indices(full_times, times)

    if cfg.group == GROUP_SURFACE:
        missing = ds_g[cfg.target_variable].isel(time=slice(start_idx, end_idx)).isnull().any().compute()
        return not bool(missing)

    if cfg.level_dim_out is None or cfg.level_indices_write is None:
        raise ValueError("Missing level config for completion check.")

    missing = (
        ds_g[cfg.target_variable]
        .isel(time=slice(start_idx, end_idx), **{cfg.level_dim_out: cfg.level_indices_write})
        .isnull()
        .any()
        .compute()
    )
    return not bool(missing)


def write_step(cfg: JobConfig, full_times: pd.DatetimeIndex, step_ts: pd.Timestamp) -> str:
    step_label = step_ts.strftime("%Y-%m-%d")
    print(f"[day] {step_label} build start", flush=True)
    ds_step = build_day_dataset(cfg, step_ts)
    if ds_step is None or ds_step.sizes.get("time", 0) == 0:
        return f"[skip] {step_label} no data"

    if cfg.skip_complete_steps and is_step_complete(cfg, full_times, ds_step):
        return f"[skip] {step_label} complete"

    times = pd.DatetimeIndex(ds_step.time.values)
    start_idx, end_idx = month_region_indices(full_times, times)

    sync = zarr.ProcessSynchronizer(cfg.sync_path)

    if cfg.group == GROUP_SURFACE:
        ds_write = ds_step[[cfg.target_variable]]
        region_dims = {"time"}
        drop_vars = [
            name
            for name, var in ds_write.variables.items()
            if name != cfg.target_variable and set(var.dims).isdisjoint(region_dims)
        ]
        if drop_vars:
            ds_write = ds_write.drop_vars(drop_vars)

        print(f"[day] {step_label} write start time[{start_idx}:{end_idx}]", flush=True)
        ds_write.to_zarr(
            cfg.output_zarr,
            group=cfg.group,
            mode="a",
            region={"time": slice(start_idx, end_idx)},
            consolidated=False,
            zarr_format=2,
            safe_chunks=False,
            align_chunks=True,
            synchronizer=sync,
        )
        return f"[write] {step_label} {cfg.target_variable} -> {cfg.group} time[{start_idx}:{end_idx}]"

    if cfg.level_dim_out is None or cfg.level_indices_write is None:
        raise ValueError("Missing level config for pl/ml write.")

    blocks = contiguous_blocks(cfg.level_indices_write)
    print(
        f"[day] {step_label} write start time[{start_idx}:{end_idx}] "
        f"{cfg.level_dim_out}_values={[int(x) for x in np.asarray(cfg.level_values_write).tolist()]}",
        flush=True,
    )
    for pos_start, pos_end, idx_start, idx_end in blocks:
        block_ds = ds_step[[cfg.target_variable]].isel({cfg.level_dim_out: slice(pos_start, pos_end)})
        region_dims = {"time", cfg.level_dim_out}
        drop_vars = [
            name
            for name, var in block_ds.variables.items()
            if name != cfg.target_variable and set(var.dims).isdisjoint(region_dims)
        ]
        if drop_vars:
            block_ds = block_ds.drop_vars(drop_vars)

        block_ds.to_zarr(
            cfg.output_zarr,
            group=cfg.group,
            mode="a",
            region={
                "time": slice(start_idx, end_idx),
                cfg.level_dim_out: slice(idx_start, idx_end),
            },
            consolidated=False,
            zarr_format=2,
            safe_chunks=False,
            align_chunks=True,
            synchronizer=sync,
        )

    return (
        f"[write] {step_label} {cfg.target_variable} -> {cfg.group} "
        f"time[{start_idx}:{end_idx}] "
        f"{cfg.level_dim_out}_values={[int(x) for x in np.asarray(cfg.level_values_write).tolist()]} "
        f"{cfg.level_dim_out}_indices={cfg.level_indices_write.tolist()}"
    )


def infer_group(
    group_arg: str,
    pressure_level: Optional[int],
    model_level: Optional[int],
    pressure_levels: Sequence[int],
    model_levels: Sequence[int],
) -> str:
    group_arg = normalize_group_label(group_arg)
    has_pl = pressure_level is not None or len(pressure_levels) > 0
    has_ml = model_level is not None or len(model_levels) > 0
    if has_pl and has_ml:
        raise ValueError("Provide pressure-level args or model-level args, not both.")
    if has_pl:
        return GROUP_PRESSURE
    if has_ml:
        return GROUP_MODEL
    if group_arg == "auto":
        return GROUP_SURFACE
    if group_arg not in GROUP_CHOICES:
        raise ValueError(f"Invalid group: {group_arg}")
    return group_arg


def resolve_level_config(args: argparse.Namespace, group: str) -> Tuple[Optional[str], Optional[np.ndarray], Optional[np.ndarray], Optional[np.ndarray]]:
    if group == GROUP_SURFACE:
        return None, None, None, None

    source_store = ARCO_MODEL_LEVEL_STORE if group == GROUP_MODEL else ARCO_SURFACE_PRESSURE_STORE
    ds = open_source(source_store, args.token, args.time_chunk)
    level_dim_src = detect_level_dim(ds, group)
    all_levels = np.asarray(ds[level_dim_src].values)

    out_dim = "level" if group == GROUP_PRESSURE else "model_level"

    requested: List[int] = []
    pressure_all = str(args.pressure_levels or "").strip().lower() == "all"
    model_all = str(args.model_levels or "").strip().lower() == "all"
    if group == GROUP_PRESSURE:
        if args.pressure_level is not None:
            requested.append(int(args.pressure_level))
        if args.pressure_levels is not None and not pressure_all:
            requested.extend(parse_int_list(args.pressure_levels))
    else:
        if args.model_level is not None:
            requested.append(int(args.model_level))
        if args.model_levels is not None and not model_all:
            requested.extend(parse_int_list(args.model_levels))

    if group == GROUP_PRESSURE and (args.all_levels or pressure_all or len(requested) == 0):
        requested = [int(x) for x in np.asarray(all_levels).tolist()]
    elif group == GROUP_MODEL and (args.all_levels or model_all):
        requested = [int(x) for x in np.asarray(all_levels).tolist()]

    if len(requested) == 0:
        raise ValueError(
            f"For group '{group}', pass level(s) via --pressure-level/--pressure-levels or "
            f"--model-level/--model-levels, or use --all-levels."
        )

    level_values_write, level_indices_write = map_levels_to_indices(all_levels, requested)
    return out_dim, all_levels, level_values_write, level_indices_write


def build_config(args: argparse.Namespace) -> JobConfig:
    pressure_levels = parse_int_list(args.pressure_levels)
    model_levels = parse_int_list(args.model_levels)

    group = infer_group(args.group, args.pressure_level, args.model_level, pressure_levels, model_levels)
    source_store = ARCO_MODEL_LEVEL_STORE if group == GROUP_MODEL else ARCO_SURFACE_PRESSURE_STORE
    registry_entries = load_registry_entries(args.registry_file)
    source_map = build_group_source_map(registry_entries, group)
    registry_entry = lookup_registry_entry(registry_entries, group, args.var_name)
    source_variable = resolve_source_var(args.var_name, args.source_var, source_map)
    target_variable = args.target_var if args.target_var else args.var_name

    level_dim_out, level_values_all, level_values_write, level_indices_write = resolve_level_config(args, group)

    return JobConfig(
        variable=args.var_name,
        source_variable=source_variable,
        target_variable=target_variable,
        group=group,
        source_store=source_store,
        process_start=pd.Timestamp(args.process_start),
        process_end=pd.Timestamp(args.process_end),
        output_zarr=args.output_zarr,
        sync_path=args.sync_path,
        token=args.token,
        time_chunk=args.time_chunk,
        level_chunk=args.level_chunk,
        skip_complete_steps=not args.overwrite_steps,
        level_dim_out=level_dim_out,
        level_values_all=level_values_all,
        level_values_write=level_values_write,
        level_indices_write=level_indices_write,
        source_map=source_map,
        registry_long_name=registry_entry.long_name if registry_entry else None,
        registry_units=registry_entry.units if registry_entry else None,
    )


def run(cfg: JobConfig, full_start_year: int, full_end_year: int, n_jobs: int, consolidate: bool) -> None:
    full_times = full_time_axis(full_start_year, full_end_year)
    steps = pd.date_range(
        cfg.process_start.normalize(),
        cfg.process_end.normalize(),
        freq="D",
    )

    if len(steps) == 0:
        print("[skip] no days in requested process range", flush=True)
        return

    print(
        f"[run] var={cfg.variable} source={cfg.source_variable} target={cfg.target_variable} "
        f"group={cfg.group} days={len(steps)} range={steps[0].strftime('%Y-%m-%d')}..{steps[-1].strftime('%Y-%m-%d')}"
        f"{format_level_summary(cfg)}",
        flush=True,
    )
    print("[run] ensuring output group/variable", flush=True)
    ensure_group_and_variable(cfg, full_times, steps)
    print("[run] initialization check complete", flush=True)

    workers = max(1, min(int(n_jobs), len(steps)))
    print(f"[run] daily workers={workers}", flush=True)
    for i in range(0, len(steps), workers):
        batch = steps[i:i + workers]
        print(
            f"[run] launching batch {i // workers + 1}: "
            f"{batch[0].strftime('%Y-%m-%d')}..{batch[-1].strftime('%Y-%m-%d')}",
            flush=True,
        )
        results = Parallel(n_jobs=workers, backend="loky")(delayed(write_step)(cfg, full_times, ts) for ts in batch)
        for line in results:
            print(line, flush=True)

    if consolidate:
        zarr.consolidate_metadata(cfg.output_zarr)
        print(f"[meta] consolidated metadata: {cfg.output_zarr}", flush=True)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Process Google ARCO ERA5 to grouped NYS Zarr (sl/pl/ml).")
    parser.add_argument("--var-name", required=True, help="Target variable name (e.g., t2m, tp, i10fg, u, v, q).")
    parser.add_argument("--source-var", default=None, help="Optional source variable name if different from --var-name.")
    parser.add_argument("--target-var", default=None, help="Optional output variable name override. Defaults to --var-name.")
    parser.add_argument(
        "--registry-file",
        default=DEFAULT_REGISTRY_FILE,
        help="CSV mapping file with columns: var_name,source_var,group (optional).",
    )
    parser.add_argument(
        "--group",
        default="auto",
        choices=["auto", GROUP_SURFACE, GROUP_PRESSURE, GROUP_MODEL, "sf"],
        help="Target group. auto=>sl unless level args are passed. Legacy 'sf' is accepted.",
    )

    parser.add_argument("--pressure-level", type=int, default=None, help="Single pressure level value (legacy).")
    parser.add_argument("--pressure-levels", default=None, help="Pressure levels list, comma-list, or 'all'. Defaults to all levels for group=pl.")
    parser.add_argument("--model-level", type=int, default=None, help="Single model level value (legacy).")
    parser.add_argument("--model-levels", default=None, help="Model levels list, comma-list, or 'all'.")
    parser.add_argument("--all-levels", action="store_true", help="Process all levels for pl/ml.")

    parser.add_argument("--process-start", default="2018-01-01", help="Process start datetime (inclusive).")
    parser.add_argument("--process-end", default="2028-12-31 23:00:00", help="Process end datetime (inclusive).")
    parser.add_argument("--full-start-year", type=int, default=1940, help="Global full time axis start year.")
    parser.add_argument("--full-end-year", type=int, default=2050, help="Global full time axis end year.")
    parser.add_argument("--output-zarr", default=DEFAULT_OUTPUT_ZARR, help="Output root Zarr path.")
    parser.add_argument("--sync-path", default=None, help="Path for ProcessSynchronizer lock file.")
    parser.add_argument("--token", default="anon", help="GCS token mode for ARCO read access.")
    parser.add_argument("--time-chunk", type=int, default=TIME_CHUNK, help="Time chunk for reads/writes.")
    parser.add_argument("--level-chunk", type=int, default=LEVEL_CHUNK, help="Level chunk for pl/ml arrays.")
    parser.add_argument("--n-jobs", type=int, default=0, help="Daily parallel workers (0=>SLURM_CPUS_ON_NODE or cpu_count).")
    parser.add_argument("--overwrite-steps", action="store_true", help="Overwrite daily slices even if already complete.")
    parser.add_argument("--consolidate-metadata", action="store_true", help="Consolidate metadata after run.")
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    if args.sync_path is None:
        args.sync_path = f"{args.output_zarr}.sync"

    cfg = build_config(args)
    jobs = args.n_jobs if args.n_jobs and args.n_jobs > 0 else int(
        os.environ.get("SLURM_CPUS_ON_NODE", os.cpu_count() or 1)
    )
    print(
        f"[args] var={args.var_name} group={args.group} process_start={args.process_start} "
        f"process_end={args.process_end} output={args.output_zarr} n_jobs={jobs}",
        flush=True,
    )

    run(
        cfg=cfg,
        full_start_year=args.full_start_year,
        full_end_year=args.full_end_year,
        n_jobs=jobs,
        consolidate=args.consolidate_metadata,
    )
