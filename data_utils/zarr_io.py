"""
Shared zarr management and unit conversion for met-data-products process scripts.

Public API
----------
load_var_meta()           → dict
target_units(var_name)    → str
target_long_name(var_name)→ str
normalize_units(units)    → str | None
apply_var_attrs(ds, var_name) → xr.Dataset
convert_units_numpy(data, var_name, src_units) → np.ndarray
open_zarr_safe(zarr_store, synchronizer, ...) → xr.Dataset
init_zarr(output_zarr, full_times, ref_ds, var_name, chunks, ...) → None
ensure_store(output_zarr, full_times, var_name, get_template_fn, chunks, ...) → xr.Dataset
has_missing_data(zarr_store, times, var_name, synchronizer) → bool
write_region(ds, output_zarr, full_times, chunks, synchronizer) → None
get_slurm_cpus() → int
parallel_batch(fn, items, n_jobs, batch_size, desc) → None
"""

from __future__ import annotations

import os
import time
from pathlib import Path
from typing import Callable, Dict, Optional

import dask.array as da
import numpy as np
import pandas as pd
import xarray as xr
import yaml
import zarr
from joblib import Parallel, delayed
from tqdm import tqdm

_VAR_META_PATH = Path(__file__).with_name("var_meta.yaml")
_VAR_META_CACHE: Optional[dict] = None


# ---------------------------------------------------------------------------
# Metadata helpers
# ---------------------------------------------------------------------------

def load_var_meta() -> dict:
    global _VAR_META_CACHE
    if _VAR_META_CACHE is None:
        with open(_VAR_META_PATH) as f:
            _VAR_META_CACHE = yaml.safe_load(f)
    return _VAR_META_CACHE


def target_units(var_name: str) -> str:
    return load_var_meta()["variables"].get(var_name, {}).get("units", "")


def target_long_name(var_name: str) -> str:
    return load_var_meta()["variables"].get(var_name, {}).get("long_name", var_name)


def normalize_units(units: Optional[str]) -> Optional[str]:
    """Normalize notation variants to a canonical string (case-insensitive key lookup)."""
    if not units:
        return units
    key = " ".join(units.strip().lower().split())
    aliases = load_var_meta().get("unit_aliases", {})
    return aliases.get(key, units)


# ---------------------------------------------------------------------------
# Unit conversion
# ---------------------------------------------------------------------------

def _get_conversion_factor(var_name: str, src_units: str) -> float:
    """Return the scale factor to convert src_units → canonical target_units for var_name."""
    meta = load_var_meta()
    conv_table = meta.get("unit_conversions", {}).get(var_name, {})
    src_norm = normalize_units(src_units)
    entry = conv_table.get(src_units) or conv_table.get(src_norm)
    if entry is None:
        raise ValueError(
            f"No unit conversion for {var_name}: {src_units!r}. "
            f"Add it to data_utils/var_meta.yaml under unit_conversions."
        )
    return float(entry[1])


def apply_var_attrs(ds: xr.Dataset, var_name: str) -> xr.Dataset:
    """
    Convert var_name to canonical units (if needed) and set canonical attrs.
    Reads src_units from ds[var_name].attrs['units']. No-op if var_name not in ds.
    """
    if var_name not in ds.data_vars:
        return ds
    canonical_units = target_units(var_name)
    canonical_long_name = target_long_name(var_name)
    src_units = ds[var_name].attrs.get("units", "")
    if src_units and canonical_units:
        src_norm = normalize_units(src_units)
        tgt_norm = normalize_units(canonical_units)
        if src_norm != tgt_norm:
            factor = _get_conversion_factor(var_name, src_units)
            if factor != 1.0:
                ds = ds.copy()
                ds[var_name] = ds[var_name] * factor
    attrs = dict(ds[var_name].attrs)
    attrs["long_name"] = canonical_long_name
    attrs["units"] = canonical_units
    attrs["_FillValue"] = np.nan
    attrs["missing_value"] = np.nan
    ds[var_name].attrs = attrs
    return ds


def convert_units_numpy(
    data: np.ndarray,
    var_name: str,
    src_units: Optional[str],
) -> np.ndarray:
    """Convert a numpy array from src_units to canonical target_units for var_name."""
    canonical = target_units(var_name)
    if not src_units or not canonical:
        return data
    src_norm = normalize_units(src_units)
    tgt_norm = normalize_units(canonical)
    if src_norm == tgt_norm:
        return data
    factor = _get_conversion_factor(var_name, src_units)
    if factor == 1.0:
        return data
    return np.asarray(data * factor, dtype=np.float32)


# ---------------------------------------------------------------------------
# Zarr I/O
# ---------------------------------------------------------------------------

def open_zarr_safe(
    zarr_store: str,
    synchronizer=None,
    attempts: int = 5,
    base_delay: float = 1.0,
) -> xr.Dataset:
    """Open a zarr store, retrying on NFS stale file handle errors (errno 116)."""
    kwargs: Dict = {"consolidated": False}
    if synchronizer is not None:
        kwargs["synchronizer"] = synchronizer
    for attempt in range(1, attempts + 1):
        try:
            return xr.open_zarr(zarr_store, **kwargs)
        except OSError as exc:
            if getattr(exc, "errno", None) != 116 or attempt == attempts:
                raise
            time.sleep(base_delay * attempt)


def init_zarr(
    output_zarr: str,
    full_times: pd.DatetimeIndex,
    ref_ds: xr.Dataset,
    var_name: str,
    chunks: Dict,
    mode: str = "w",
    write_global_attrs: bool = False,
    global_title: str = "",
    extra_var_attrs: Optional[Dict] = None,
    synchronizer=None,
) -> None:
    """
    Create (mode='w') or append (mode='a') a NaN-filled zarr variable.

    Spatial dims and coords are taken from ref_ds (time dim is excluded from spatial).
    Canonical long_name and units come from var_meta.yaml.
    chunks must contain 'time' and each spatial dim key.
    """
    spatial_dims = [d for d in ref_ds.dims if d != "time"]
    spatial_sizes = {d: ref_ds.sizes[d] for d in spatial_dims}
    all_dims = ["time"] + spatial_dims

    chunk_tuple = tuple(
        chunks.get(d, spatial_sizes.get(d, len(full_times) if d == "time" else 1))
        for d in all_dims
    )
    shape = (len(full_times),) + tuple(spatial_sizes[d] for d in spatial_dims)
    data = da.full(shape, np.nan, chunks=chunk_tuple, dtype=np.float32)

    coords: Dict = {"time": full_times}
    for k, v in ref_ds.coords.items():
        if k != "time":
            coords[k] = v

    ds_init = xr.Dataset(
        {var_name: xr.DataArray(data, dims=all_dims)},
        coords=coords,
    )
    if write_global_attrs:
        ds_init.attrs = {
            "title": global_title or "NYS Meteorological Dataset",
            "Conventions": "CF-1.8",
            "history": "Initialized empty Zarr store",
        }

    # extra_var_attrs as base; canonical long_name/units/_FillValue always win.
    var_attrs: Dict = dict(extra_var_attrs or {})
    var_attrs["long_name"] = target_long_name(var_name)
    var_attrs["units"] = target_units(var_name)
    var_attrs["_FillValue"] = np.nan
    var_attrs["missing_value"] = np.nan
    ds_init[var_name].attrs = var_attrs

    kwargs: Dict = {"mode": mode, "compute": False, "zarr_format": 2, "consolidated": False}
    if synchronizer is not None:
        kwargs["synchronizer"] = synchronizer
    ds_init.to_zarr(output_zarr, **kwargs)


def ensure_store(
    output_zarr: str,
    full_times: pd.DatetimeIndex,
    var_name: str,
    get_template_fn: Callable[[], xr.Dataset],
    chunks: Dict,
    global_title: str = "",
    extra_var_attrs: Optional[Dict] = None,
    synchronizer=None,
) -> xr.Dataset:
    """
    Ensure zarr store exists with var_name initialized and time axis consistent.

    get_template_fn() is called lazily to obtain spatial structure when needed.
    Returns the opened zarr store.
    """
    if os.path.exists(output_zarr):
        ds = open_zarr_safe(output_zarr, synchronizer)
        if not np.array_equal(
            pd.to_datetime(ds.time.values),
            pd.to_datetime(full_times.values),
        ):
            raise ValueError("Time axis mismatch in existing Zarr store.")
        if var_name not in ds.data_vars:
            ref_ds = get_template_fn()
            init_zarr(
                output_zarr, full_times, ref_ds, var_name, chunks,
                mode="a", write_global_attrs=False,
                extra_var_attrs=extra_var_attrs, synchronizer=synchronizer,
            )
        return ds

    ref_ds = get_template_fn()
    init_zarr(
        output_zarr, full_times, ref_ds, var_name, chunks,
        mode="w", write_global_attrs=True, global_title=global_title,
        extra_var_attrs=extra_var_attrs, synchronizer=synchronizer,
    )
    return open_zarr_safe(output_zarr, synchronizer)


def has_missing_data(
    zarr_store: str,
    times: pd.DatetimeIndex,
    var_name: str,
    synchronizer=None,
) -> bool:
    """Return True if any timestamp in times has missing (NaN) values for var_name."""
    ds = open_zarr_safe(zarr_store, synchronizer)
    if var_name not in ds.data_vars:
        return True
    data = ds[var_name].reindex(time=times)
    return bool(data.isnull().any().compute())


def write_region(
    ds: xr.Dataset,
    output_zarr: str,
    full_times: pd.DatetimeIndex,
    chunks: Dict,
    synchronizer=None,
) -> None:
    """
    Rechunk → searchsorted time region → drop non-dim coords → to_zarr(mode='a').
    Works for any spatial layout: (y, x), (latitude, longitude), or (values,).
    """
    chunk_dict = {dim: chunks.get(dim, ds.sizes[dim]) for dim in ds.dims}
    ds = ds.chunk(chunk_dict)

    times = pd.DatetimeIndex(ds.time.values)
    start_idx = int(np.searchsorted(full_times.values, times[0].to_datetime64()))
    end_idx = int(np.searchsorted(full_times.values, times[-1].to_datetime64())) + 1
    region = {"time": slice(start_idx, end_idx)}

    drop = [c for c in ds.coords if c != "time" and c not in ds.dims]
    if drop:
        ds = ds.drop_vars(drop)

    kwargs: Dict = {
        "mode": "a",
        "region": region,
        "consolidated": False,
        "zarr_format": 2,
        "align_chunks": True,
        "safe_chunks": False,
    }
    if synchronizer is not None:
        kwargs["synchronizer"] = synchronizer
    ds.to_zarr(output_zarr, **kwargs)


# ---------------------------------------------------------------------------
# Utilities
# ---------------------------------------------------------------------------

def get_slurm_cpus() -> int:
    """Return SLURM_CPUS_ON_NODE, falling back to os.cpu_count()."""
    return int(os.environ.get("SLURM_CPUS_ON_NODE", os.cpu_count() or 1))


def parallel_batch(
    fn: Callable,
    items: list,
    n_jobs: int,
    batch_size: int,
    desc: str = "",
) -> None:
    """tqdm outer loop over batches, joblib Parallel within each batch."""
    for i in tqdm(range(0, len(items), batch_size), desc=desc):
        batch = items[i: i + batch_size]
        Parallel(n_jobs=n_jobs, backend="loky", verbose=0)(
            delayed(fn)(item) for item in batch
        )
