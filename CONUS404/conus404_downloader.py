#!/usr/bin/env python3
"""
Download CONUS404 NYS subset directly into a local Zarr store via region writes.
"""

from __future__ import annotations

import argparse
import os
import time
from pathlib import Path
from typing import Iterable, List

import dask.array as da
import numpy as np
import pandas as pd
import planetary_computer
import pystac_client
import xarray as xr
import zarr
from joblib import Parallel, delayed

STAC_URL = "https://planetarycomputer.microsoft.com/api/stac/v1"
COLLECTION_ID = "conus404"
ASSET_KEY = "zarr-abfs"

ROOT_DIR = Path("/network/rit/lab/basulab/Projects/DFS/DATA/CONUS404_NYS")
OUTPUT_ZARR = ROOT_DIR / "CONUS404_NYS.zarr"
ZARR_SYNC_PATH = f"{OUTPUT_ZARR}.sync"
ZARR_SYNC = zarr.ProcessSynchronizer(ZARR_SYNC_PATH)

TIME_CHUNK = 24

# New York State grid indices
I1 = 1045
I2 = 1215
J1 = 610
J2 = 740
NY_INDICES = {"south_north": slice(J1, J2), "west_east": slice(I1, I2)}

DEFAULT_VARS = ["U10", "V10", "USHR6", "VSHR6", "SBCAPE", "MLCAPE", "MUCAPE"]


def parse_vars(values: Iterable[str] | None) -> List[str]:
    if not values:
        return list(DEFAULT_VARS)
    out: List[str] = []
    for item in values:
        for part in (p.strip() for p in item.split(",")):
            if part and part not in out:
                out.append(part)
    return out


def _looks_date_only(text: str) -> bool:
    return "T" not in text and ":" not in text and len(text.strip()) <= 10


def parse_process_bounds(start_text: str, end_text: str) -> tuple[pd.Timestamp, pd.Timestamp]:
    start = pd.Timestamp(start_text)
    end = pd.Timestamp(end_text)
    if _looks_date_only(start_text):
        start = start.normalize()
    if _looks_date_only(end_text):
        end = end.normalize() + pd.Timedelta(hours=23)
    if end < start:
        raise ValueError("process_end must be >= process_start")
    return start, end


def resolve_n_jobs(n_jobs: int | None) -> int:
    if n_jobs is not None and n_jobs > 0:
        return n_jobs
    cpu_requested = (
        os.environ.get("SLURM_CPUS_PER_TASK")
        or os.environ.get("SLURM_CPUS_ON_NODE")
        or os.cpu_count()
        or 1
    )
    return max(1, min(12, int(cpu_requested)))


def build_full_times(full_start_year: int, full_end_year: int) -> pd.DatetimeIndex:
    if full_end_year < full_start_year:
        raise ValueError("full_end_year must be >= full_start_year")
    return pd.date_range(
        f"{full_start_year}-01-01T00:00:00",
        f"{full_end_year}-12-31T23:00:00",
        freq="1h",
    )


def monthly_windows(
    process_start: pd.Timestamp, process_end: pd.Timestamp
) -> List[tuple[pd.Timestamp, pd.Timestamp]]:
    month_starts = pd.date_range(
        process_start.replace(day=1, hour=0, minute=0, second=0, microsecond=0, nanosecond=0),
        process_end.replace(day=1, hour=0, minute=0, second=0, microsecond=0, nanosecond=0),
        freq="MS",
    )
    windows: List[tuple[pd.Timestamp, pd.Timestamp]] = []
    for month_start in month_starts:
        month_end = (month_start + pd.offsets.MonthEnd(0)) + pd.Timedelta(hours=23)
        win_start = max(process_start, month_start)
        win_end = min(process_end, month_end)
        if win_end >= win_start:
            windows.append((win_start, win_end))
    return windows


def open_source_subset(var_list: List[str]) -> xr.Dataset:
    print("Initializing connection to Microsoft Planetary Computer...")
    cat = pystac_client.Client.open(STAC_URL, modifier=planetary_computer.sign_inplace)
    col = cat.get_collection(COLLECTION_ID)
    asset = col.assets[ASSET_KEY]

    print("Opening CONUS404 source zarr...")
    ds_all = xr.open_zarr(
        asset.href,
        storage_options=asset.extra_fields.get("xarray:storage_options", {}),
        **asset.extra_fields.get("xarray:open_kwargs", {}),
    )
    ds = ds_all[var_list].isel(**NY_INDICES)
    return clear_encoding_chunks(ds)


def clear_encoding_chunks(ds: xr.Dataset) -> xr.Dataset:
    for da_name in ds.data_vars:
        ds[da_name].encoding.pop("chunks", None)
    for coord_name in ds.coords:
        ds[coord_name].encoding.pop("chunks", None)
    return ds


def open_zarr_safe(path: Path, attempts: int = 5, base_delay: float = 1.0) -> xr.Dataset:
    for attempt in range(1, attempts + 1):
        try:
            return xr.open_zarr(path, consolidated=False, synchronizer=ZARR_SYNC)
        except OSError as exc:
            if getattr(exc, "errno", None) != 116 or attempt == attempts:
                raise
            time.sleep(base_delay * attempt)


def _build_init_dataset(
    full_times: pd.DatetimeIndex,
    template_var: xr.DataArray,
    var_name: str,
    write_global_attrs: bool,
) -> xr.Dataset:
    spatial = template_var.isel(time=0, drop=True)
    dims = ("time",) + spatial.dims
    shape = (full_times.size,) + tuple(spatial.sizes[d] for d in spatial.dims)
    chunks = (TIME_CHUNK,) + tuple(spatial.sizes[d] for d in spatial.dims)
    data = da.full(shape, np.nan, chunks=chunks, dtype=np.float32)

    ds_init = xr.Dataset(
        {var_name: xr.DataArray(data, dims=dims)},
        coords={"time": full_times},
    )
    for coord_name, coord in spatial.coords.items():
        if "time" in coord.dims:
            continue
        ds_init = ds_init.assign_coords({coord_name: coord})

    attrs = dict(template_var.attrs)
    attrs.setdefault("_FillValue", np.nan)
    attrs.setdefault("missing_value", np.nan)
    ds_init[var_name].attrs = attrs

    if write_global_attrs:
        ds_init.attrs = {
            "title": "CONUS404 hourly NYS subset",
            "source": "CONUS404 via Microsoft Planetary Computer",
            "Conventions": "CF-1.8",
            "history": "Initialized empty Zarr store for region writes",
            "grid_indices": f"south_north[{J1}:{J2}], west_east[{I1}:{I2}]",
        }

    return ds_init


def init_var_in_store(
    full_times: pd.DatetimeIndex,
    template_var: xr.DataArray,
    var_name: str,
    mode: str,
    write_global_attrs: bool,
) -> None:
    ds_init = _build_init_dataset(full_times, template_var, var_name, write_global_attrs)
    ds_init.to_zarr(
        OUTPUT_ZARR,
        mode=mode,
        compute=False,
        consolidated=False,
        zarr_format=2,
        synchronizer=ZARR_SYNC,
    )


def validate_store_time_axis(existing: xr.Dataset, full_times: pd.DatetimeIndex) -> None:
    if "time" not in existing.coords:
        raise ValueError("Existing Zarr store is missing 'time' coordinate.")
    existing_time = pd.to_datetime(existing.time.values)
    if not np.array_equal(existing_time.values, full_times.values):
        raise ValueError("Time axis mismatch in existing Zarr store.")


def validate_existing_var_shape(
    existing: xr.Dataset,
    template_var: xr.DataArray,
    var_name: str,
    full_times: pd.DatetimeIndex,
) -> None:
    if var_name not in existing.data_vars:
        return
    arr = existing[var_name]
    if "time" not in arr.dims:
        raise ValueError(f"{var_name} exists but has no 'time' dimension.")
    if arr.sizes["time"] != full_times.size:
        raise ValueError(
            f"{var_name} time dimension mismatch: {arr.sizes['time']} != {full_times.size}"
        )
    expected_spatial_dims = tuple(d for d in template_var.dims if d != "time")
    actual_spatial_dims = tuple(d for d in arr.dims if d != "time")
    if actual_spatial_dims != expected_spatial_dims:
        raise ValueError(
            f"{var_name} spatial dims mismatch: {actual_spatial_dims} != {expected_spatial_dims}"
        )
    for dim in expected_spatial_dims:
        if arr.sizes[dim] != template_var.sizes[dim]:
            raise ValueError(
                f"{var_name} size mismatch for {dim}: {arr.sizes[dim]} != {template_var.sizes[dim]}"
            )


def ensure_store_and_vars(full_times: pd.DatetimeIndex, ds_template: xr.Dataset, var_list: List[str]) -> None:
    ROOT_DIR.mkdir(parents=True, exist_ok=True)
    if OUTPUT_ZARR.exists():
        existing = open_zarr_safe(OUTPUT_ZARR)
        validate_store_time_axis(existing, full_times)
        for var in var_list:
            validate_existing_var_shape(existing, ds_template[var], var, full_times)
            if var not in existing.data_vars:
                init_var_in_store(full_times, ds_template[var], var, mode="a", write_global_attrs=False)
                print(f"[init] added variable {var}")
        return

    first = True
    for var in var_list:
        init_var_in_store(
            full_times,
            ds_template[var],
            var,
            mode="w" if first else "a",
            write_global_attrs=first,
        )
        print(f"[init] created store variable {var}")
        first = False


def time_region_from_full_axis(full_times: pd.DatetimeIndex, times: pd.DatetimeIndex) -> dict[str, slice]:
    if len(times) == 0:
        raise ValueError("No timestamps to write.")
    start_idx = int(np.searchsorted(full_times.values, times.values[0]))
    end_idx = int(np.searchsorted(full_times.values, times.values[-1])) + 1
    if start_idx < 0 or end_idx > full_times.size:
        raise ValueError("Requested time range is outside full time axis.")
    if not np.array_equal(full_times.values[start_idx:end_idx], times.values):
        raise ValueError("Requested times are not an exact subset of full time axis.")
    return {"time": slice(start_idx, end_idx)}


def region_fill_status(var_name: str, times: pd.DatetimeIndex) -> str:
    ds = open_zarr_safe(OUTPUT_ZARR)
    if var_name not in ds.data_vars:
        return "missing_var"
    target = ds[var_name].reindex(time=times)
    missing_any = bool(target.isnull().any().compute())
    if not missing_any:
        return "full"
    missing_all = bool(target.isnull().all().compute())
    if missing_all:
        return "empty"
    return "partial"


def prepare_write_dataset(ds_window: xr.Dataset, var_name: str) -> xr.Dataset:
    da_var = ds_window[var_name].astype(np.float32)
    da_var = da_var.transpose("time", "south_north", "west_east")

    chunk_map = {"time": min(TIME_CHUNK, da_var.sizes["time"])}
    for dim in ("south_north", "west_east"):
        if dim in da_var.dims:
            chunk_map[dim] = da_var.sizes[dim]
    da_var = da_var.chunk(chunk_map)

    write_ds = xr.Dataset({var_name: da_var})
    coords_to_drop = [c for c in write_ds.coords if c != "time"]
    if coords_to_drop:
        write_ds = write_ds.drop_vars(coords_to_drop, errors="ignore")
    return clear_encoding_chunks(write_ds.assign_coords(time=ds_window.time))


def write_region(ds_window: xr.Dataset, full_times: pd.DatetimeIndex, var_name: str) -> None:
    times = pd.DatetimeIndex(pd.to_datetime(ds_window.time.values))
    region = time_region_from_full_axis(full_times, times)
    write_ds = prepare_write_dataset(ds_window, var_name)
    write_ds.to_zarr(
        OUTPUT_ZARR,
        mode="a",
        region=region,
        consolidated=False,
        zarr_format=2,
        safe_chunks=False,
        synchronizer=ZARR_SYNC,
    )
    print(f"[write] {var_name} -> {region} ({times[0]} to {times[-1]})")


def process_one_month_window(
    ds_region: xr.Dataset,
    month_start: pd.Timestamp,
    month_end: pd.Timestamp,
    var_list: List[str],
    full_times: pd.DatetimeIndex,
    overwrite: bool,
) -> None:
    ds_window = ds_region.sel(time=slice(month_start, month_end)).sortby("time")
    if ds_window.sizes.get("time", 0) == 0:
        print(f"[skip] no data in monthly window {month_start} to {month_end}")
        return

    times = pd.DatetimeIndex(pd.to_datetime(ds_window.time.values))
    _ = time_region_from_full_axis(full_times, times)
    month_tag = f"{month_start:%Y-%m}"
    print(f"[month] {month_tag}: {times[0]} to {times[-1]} ({len(times)} hours)")

    for var in var_list:
        if overwrite:
            write_region(ds_window[[var]], full_times, var)
            continue

        status = region_fill_status(var, times)
        if status in {"empty", "partial"}:
            write_region(ds_window[[var]], full_times, var)
        elif status == "full":
            print(f"[skip] {var} {month_tag}: requested region already filled")
        else:
            print(f"[skip] {var} {month_tag}: unexpected region status={status}")


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Download CONUS404 NYS subset and write directly to a local Zarr store."
    )
    parser.add_argument(
        "--process-start",
        required=True,
        help="Start date/time inclusive (e.g., 2017-01-01 or 2017-01-01T00)",
    )
    parser.add_argument(
        "--process-end",
        required=True,
        help="End date/time inclusive (e.g., 2017-01-31 or 2017-01-31T23)",
    )
    parser.add_argument(
        "--full-start-year",
        type=int,
        required=True,
        help="Start year for full hourly zarr time axis (inclusive).",
    )
    parser.add_argument(
        "--full-end-year",
        type=int,
        required=True,
        help="End year for full hourly zarr time axis (inclusive).",
    )
    parser.add_argument(
        "--var-list",
        "--vars",
        dest="var_list",
        nargs="+",
        default=None,
        help="Variables to write (space or comma separated).",
    )
    parser.add_argument(
        "--overwrite",
        action="store_true",
        help="Overwrite requested region even if data already exist.",
    )
    parser.add_argument(
        "--n-jobs",
        type=int,
        default=None,
        help="Internal monthly parallel jobs. Default: min(8, requested CPUs).",
    )
    args = parser.parse_args()

    var_list = parse_vars(args.var_list)
    process_start, process_end = parse_process_bounds(args.process_start, args.process_end)
    full_times = build_full_times(args.full_start_year, args.full_end_year)
    n_jobs = resolve_n_jobs(args.n_jobs)
    windows = monthly_windows(process_start, process_end)

    print("=" * 80)
    print("CONUS404 -> ZARR REGIONAL WRITER (NYS)")
    print("=" * 80)
    print(f"Process window:     {process_start} to {process_end}")
    print(f"Full time axis:     {args.full_start_year}-01-01 00:00 to {args.full_end_year}-12-31 23:00")
    print(f"Variables:          {', '.join(var_list)}")
    print(f"Overwrite:          {args.overwrite}")
    print(f"Internal n_jobs:    {n_jobs}")
    print(f"Monthly windows:    {len(windows)}")
    print(f"Output zarr:        {OUTPUT_ZARR}")
    print(f"Grid subset:        south_north[{J1}:{J2}], west_east[{I1}:{I2}]")
    print("=" * 80)

    ds_region = open_source_subset(var_list)
    ds_requested = ds_region.sel(time=slice(process_start, process_end)).sortby("time")
    if ds_requested.sizes.get("time", 0) == 0:
        raise ValueError("No CONUS404 data found for requested process window.")

    ensure_store_and_vars(full_times, ds_region, var_list)
    if not windows:
        raise ValueError("No monthly windows generated for requested process range.")

    # Use threads so all workers can share the already-open source dataset handle.
    if n_jobs == 1 or len(windows) == 1:
        for month_start, month_end in windows:
            process_one_month_window(
                ds_region, month_start, month_end, var_list, full_times, args.overwrite
            )
    else:
        Parallel(n_jobs=n_jobs, backend="threading")(
            delayed(process_one_month_window)(
                ds_region, month_start, month_end, var_list, full_times, args.overwrite
            )
            for month_start, month_end in windows
        )


if __name__ == "__main__":
    main()
