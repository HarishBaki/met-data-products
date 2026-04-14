#!/usr/bin/env python3
"""
Download and save ERA5 (Destination Earth) NYS crop to a local Zarr store.
"""

from __future__ import annotations

import argparse
from pathlib import Path
from typing import Iterable, List, Sequence, Tuple

import numpy as np
import xarray as xr
import zarr

SOURCE_URL = "https://data.earthdatahub.destine.eu/era5/reanalysis-era5-single-levels-v0.zarr"
OUTPUT_DIR = Path("/network/rit/lab/basulab/Projects/DFS/DATA/ERA5_NYS")
OUTPUT_ZARR = OUTPUT_DIR / "ERA5_analysis_DestinE_NYS.zarr"

LAT_MIN = 38
LAT_MAX = 48
LON_MIN = 278
LON_MAX = 292
TIME_CHUNK = 24

DEFAULT_VARS = ["u10", "v10", "t2m", "d2m", "tp", "sp"]

DERIVED_VARS = {
    "si10": ["u10", "v10"],
    "wdir10": ["u10", "v10"],
}

VAR_LONGNAME = {
    "si10": "10 m wind speed",
    "i10fg": "10 m wind gust",
    "t2m": "2 m air temperature",
    "sp": "surface pressure",
    "d2m": "2 m dew point temperature",
    "u10": "10 m eastward wind",
    "v10": "10 m northward wind",
    "wdir10": "10 m wind direction",
    "tp": "total precipitation",
}

VAR_UNITS = {
    "si10": "m s**-1",
    "i10fg": "m s**-1",
    "t2m": "K",
    "sp": "Pa",
    "d2m": "K",
    "u10": "m s**-1",
    "v10": "m s**-1",
    "wdir10": "Degree true",
    "tp": "kg m**-2",
}

UNIT_CONVERSIONS = {
    "tp": {
        "kg m**-2": ("kg m**-2", 1.0),
        "mm": ("kg m**-2", 1.0),
        "m": ("kg m**-2", 1000.0),
    },
    "sp": {
        "Pa": ("Pa", 1.0),
        "hPa": ("Pa", 100.0),
    },
}

UNIT_ALIASES = {
    "kg m-2": "kg m**-2",
    "kg m^-2": "kg m**-2",
    "kg m**-2": "kg m**-2",
    "mm": "kg m**-2",
    "m": "m",
    "pa": "Pa",
    "hpa": "hPa",
    "m s-1": "m s**-1",
    "m s^-1": "m s**-1",
    "m s**-1": "m s**-1",
    "degree": "Degree true",
    "degrees": "Degree true",
    "degree true": "Degree true",
}


def parse_vars(values: Iterable[str] | None) -> List[str]:
    if not values:
        return DEFAULT_VARS
    vars_out: List[str] = []
    for item in values:
        parts = [p.strip() for p in item.split(",") if p.strip()]
        for p in parts:
            if p not in vars_out:
                vars_out.append(p)
    return vars_out


def open_source() -> xr.Dataset:
    return xr.open_dataset(
        SOURCE_URL,
        storage_options={"client_kwargs": {"trust_env": True}},
        chunks={},
        engine="zarr",
    )


def subset_dataset(ds: xr.Dataset, variables: List[str], start: str, end: str) -> xr.Dataset:
    ds = ds[variables]
    ds = ds.sel(latitude=slice(LAT_MAX, LAT_MIN), longitude=slice(LON_MIN, LON_MAX))
    ds = ds.sel(valid_time=slice(start, end))
    if "valid_time" in ds.dims:
        ds = ds.rename({"valid_time": "time"})
    drop_vars = [v for v in ("number", "surface") if v in ds.variables]
    if drop_vars:
        ds = ds.drop_vars(drop_vars)
    ds = ds.chunk({"time": TIME_CHUNK, "latitude": -1, "longitude": -1})
    ds = clear_encoding_chunks(ds)
    ds = apply_var_attrs_and_units(ds, variables)
    return ds


def expand_required_vars(requested: List[str]) -> List[str]:
    expanded: List[str] = []
    for var in requested:
        if var in DERIVED_VARS:
            for dep in DERIVED_VARS[var]:
                if dep not in expanded:
                    expanded.append(dep)
            continue
        if var not in expanded:
            expanded.append(var)
    return expanded


def apply_var_attrs_and_units(ds: xr.Dataset, variables: List[str]) -> xr.Dataset:
    for var in variables:
        if var not in ds.data_vars:
            continue
        attrs = dict(ds[var].attrs)
        if not attrs.get("long_name") and var in VAR_LONGNAME:
            attrs["long_name"] = VAR_LONGNAME[var]
        src_units = attrs.get("units")
        if not src_units:
            if var in VAR_UNITS:
                attrs["units"] = VAR_UNITS[var]
                ds[var].attrs = attrs
            continue
        if var not in VAR_UNITS:
            ds[var].attrs = attrs
            continue
        src_norm = normalize_units(src_units)
        target_units = VAR_UNITS[var]
        target_norm = normalize_units(target_units)
        if src_norm == target_norm:
            attrs["units"] = target_units
            ds[var].attrs = attrs
            continue
        conv = UNIT_CONVERSIONS.get(var, {}).get(src_units) or UNIT_CONVERSIONS.get(var, {}).get(src_norm)
        if conv is None:
            raise ValueError(
                f"No unit conversion available for {var}: {src_units} -> {target_units}"
            )
        out_units, factor = conv
        if factor != 1.0:
            ds[var] = ds[var] * factor
        attrs["units"] = out_units
        ds[var].attrs = attrs
    return ds


def clear_encoding_chunks(ds: xr.Dataset) -> xr.Dataset:
    for name, da in ds.data_vars.items():
        da.encoding.pop("chunks", None)
    for name, coord in ds.coords.items():
        coord.encoding.pop("chunks", None)
    return ds


def normalize_units(units: str | None) -> str | None:
    if not units:
        return units
    key = " ".join(units.strip().lower().split())
    return UNIT_ALIASES.get(key, units)


def apply_derived(ds: xr.Dataset, requested_vars: List[str]) -> xr.Dataset:
    if "si10" in requested_vars:
        if "u10" not in ds.data_vars or "v10" not in ds.data_vars:
            raise KeyError("u10/v10 required for si10")
        ds["si10"] = (ds["u10"] ** 2 + ds["v10"] ** 2) ** 0.5
    if "wdir10" in requested_vars:
        if "u10" not in ds.data_vars or "v10" not in ds.data_vars:
            raise KeyError("u10/v10 required for wdir10")
        ds["wdir10"] = (
            (270 - np.rad2deg(np.arctan2(ds["v10"], ds["u10"]))) % 360
        ).where((ds["u10"] != 0) | (ds["v10"] != 0), other=0)
    return ds


def open_existing_zarr(path: Path) -> xr.Dataset:
    try:
        return xr.open_zarr(path, consolidated=True)
    except Exception:
        return xr.open_zarr(path, consolidated=False)


def ensure_time_subset(existing: xr.Dataset, incoming: xr.Dataset) -> np.ndarray:
    if "time" not in existing.coords:
        raise ValueError("Existing Zarr store is missing a 'time' coordinate.")
    existing_index = existing["time"].to_index()
    incoming_index = incoming["time"].to_index()
    idx = existing_index.get_indexer(incoming_index)
    if (idx < 0).any():
        raise ValueError(
            "Incoming time range is not a subset of the existing Zarr time coordinate."
        )
    return idx


def contiguous_blocks(idx: np.ndarray) -> List[Tuple[int, int, int, int]]:
    blocks: List[Tuple[int, int, int, int]] = []
    if len(idx) == 0:
        return blocks
    start_pos = 0
    for i in range(1, len(idx)):
        if idx[i] != idx[i - 1] + 1:
            blocks.append((start_pos, i, idx[start_pos], idx[i - 1] + 1))
            start_pos = i
    blocks.append((start_pos, len(idx), idx[start_pos], idx[-1] + 1))
    return blocks


def chunk_key_exists(arr: zarr.Array, chunk_index: Sequence[int]) -> bool:
    if hasattr(arr, "_chunk_key"):
        key = f"{arr._key_prefix}{arr._chunk_key(chunk_index)}"
        return key in arr.store
    if hasattr(arr, "chunk_key"):
        key = f"{arr._key_prefix}{arr.chunk_key(chunk_index)}"
        return key in arr.store
    return False


def has_missing_time_chunks(
    arr: zarr.Array, time_indices: np.ndarray, time_chunk: int
) -> bool:
    if len(time_indices) == 0:
        return False
    t_chunks = np.unique(time_indices // time_chunk)
    chunk_index = [0] * arr.ndim
    for t in t_chunks:
        chunk_index[0] = int(t)
        if not chunk_key_exists(arr, chunk_index):
            return True
    return False


def ensure_var_initialized(
    zarr_group: zarr.Group, existing: xr.Dataset, ds_var: xr.DataArray
) -> None:
    name = ds_var.name
    if name in zarr_group:
        return
    dims = ds_var.dims
    shape = tuple(existing.sizes[d] for d in dims)
    if ds_var.chunks:
        chunks = tuple(c[0] for c in ds_var.chunks)
    else:
        chunks = tuple(min(existing.sizes[d], TIME_CHUNK if d == "time" else existing.sizes[d]) for d in dims)
    fill_value = ds_var.encoding.get("_FillValue", ds_var.attrs.get("_FillValue"))
    zarr_group.create_dataset(
        name,
        shape=shape,
        chunks=chunks,
        dtype=ds_var.dtype,
        fill_value=fill_value,
    )
    zarr_group[name].attrs.update(dict(ds_var.attrs))


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Download ERA5 (Destination Earth) NYS crop and save to Zarr."
    )
    parser.add_argument(
        "--start",
        required=True,
        help="Start date (e.g., 2010-01-01 or 2010).",
    )
    parser.add_argument(
        "--end",
        required=True,
        help="End date (e.g., 2025-12-31 or 2025).",
    )
    parser.add_argument(
        "--vars",
        nargs="+",
        default=None,
        help="Variables to download (space or comma separated).",
    )
    parser.add_argument(
        "--overwrite",
        action="store_true",
        help="Overwrite existing data for the requested time range.",
    )
    args = parser.parse_args()

    requested_vars = parse_vars(args.vars)
    required_vars = expand_required_vars(requested_vars)

    ds = open_source()
    ds_nys = subset_dataset(ds, required_vars, args.start, args.end)

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    if OUTPUT_ZARR.exists():
        existing = open_existing_zarr(OUTPUT_ZARR)
        time_idx = ensure_time_subset(existing, ds_nys)
        zarr_group = zarr.open_group(str(OUTPUT_ZARR), mode="a")

        for var in required_vars:
            if var not in existing.data_vars:
                ensure_var_initialized(zarr_group, existing, ds_nys[var])
                blocks = contiguous_blocks(time_idx)
                for in_start, in_end, out_start, out_end in blocks:
                    ds_block = ds_nys[[var]].isel(time=slice(in_start, in_end))
                    ds_block.to_zarr(
                        OUTPUT_ZARR,
                        mode="a",
                        region={"time": slice(out_start, out_end)},
                        consolidated=True,
                        safe_chunks=False,
                    )
                print(f"Initialized and wrote variable: {var}")
                continue

            arr = zarr_group[var]
            time_chunk = arr.chunks[0]
            needs_write = args.overwrite or has_missing_time_chunks(arr, time_idx, time_chunk)
            if not needs_write:
                print(f"{var}: requested time range already present. Skipping.")
                continue

            blocks = contiguous_blocks(time_idx)
            for in_start, in_end, out_start, out_end in blocks:
                ds_block = ds_nys[[var]].isel(time=slice(in_start, in_end))
                ds_block.to_zarr(
                    OUTPUT_ZARR,
                    mode="a",
                    region={"time": slice(out_start, out_end)},
                    consolidated=True,
                    safe_chunks=False,
                )
            print(f"{var}: wrote requested time range.")

        derived_vars = [v for v in requested_vars if v in DERIVED_VARS]
        if derived_vars:
            deps = sorted({d for v in derived_vars for d in DERIVED_VARS[v]})
            if all(dep in existing.data_vars for dep in deps):
                ds_deps = existing[deps].sel(time=ds_nys.time)
            else:
                ds_deps = ds_nys[deps]
            ds_derived = apply_derived(ds_deps, derived_vars)[derived_vars]
            ds_derived = apply_var_attrs_and_units(ds_derived, derived_vars)

            for var in derived_vars:
                if var not in existing.data_vars:
                    ensure_var_initialized(zarr_group, existing, ds_derived[var])
                    blocks = contiguous_blocks(time_idx)
                    for in_start, in_end, out_start, out_end in blocks:
                        ds_block = ds_derived[[var]].isel(time=slice(in_start, in_end))
                        ds_block.to_zarr(
                            OUTPUT_ZARR,
                            mode="a",
                            region={"time": slice(out_start, out_end)},
                            consolidated=True,
                            safe_chunks=False,
                        )
                    print(f"Initialized and wrote derived variable: {var}")
                    continue

                arr = zarr_group[var]
                time_chunk = arr.chunks[0]
                needs_write = args.overwrite or has_missing_time_chunks(
                    arr, time_idx, time_chunk
                )
                if not needs_write:
                    print(f"{var}: requested time range already present. Skipping.")
                    continue

                blocks = contiguous_blocks(time_idx)
                for in_start, in_end, out_start, out_end in blocks:
                    ds_block = ds_derived[[var]].isel(time=slice(in_start, in_end))
                    ds_block.to_zarr(
                        OUTPUT_ZARR,
                        mode="a",
                        region={"time": slice(out_start, out_end)},
                        consolidated=True,
                        safe_chunks=False,
                    )
                print(f"{var}: wrote requested time range.")
        return

    ds_nys[required_vars].to_zarr(
        OUTPUT_ZARR, mode="w", consolidated=True, safe_chunks=False
    )
    derived_vars = [v for v in requested_vars if v in DERIVED_VARS]
    if derived_vars:
        deps = sorted({d for v in derived_vars for d in DERIVED_VARS[v]})
        ds_derived = apply_derived(ds_nys[deps], derived_vars)[derived_vars]
        ds_derived = apply_var_attrs_and_units(ds_derived, derived_vars)
        ds_derived.to_zarr(OUTPUT_ZARR, mode="a", consolidated=True, safe_chunks=False)
    print(f"Wrote new Zarr store: {OUTPUT_ZARR}")


if __name__ == "__main__":
    main()
