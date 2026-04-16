# %%
"""Regrid HRRR NYS Zarr to URMA HR/LR Zarr stores.

Uses data_utils/regridding.py for consistent xESMF setup.
"""
from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path
from typing import Dict, Optional

import dask.array as da
import numpy as np
import pandas as pd
import xarray as xr
import yaml
import zarr

BOOTSTRAP_ROOT = Path(__file__).resolve().parents[1]
if str(BOOTSTRAP_ROOT) not in sys.path:
    sys.path.insert(0, str(BOOTSTRAP_ROOT))
from joblib import Parallel, delayed
from tqdm import tqdm

from data_utils.regridding import RegridderRegistry
from repo_utils import find_repo_root


OUTPUT_ROOT = "/network/rit/lab/basulab/Projects/DFS/DATA/HRRR_NYS"
SOURCE_NAME = "HRRR_NYS"
INPUT_ZARR = f"{OUTPUT_ROOT}/{SOURCE_NAME}.zarr"

REPO_ROOT = find_repo_root(__file__)
CFG_PATH = REPO_ROOT / "data_utils" / "baseline_regrid.yaml"

TIME_CHUNK = 24
Y_CHUNK = -1
X_CHUNK = -1


def is_interactive() -> bool:
    import __main__ as main

    return not hasattr(main, "__file__") or "ipykernel" in sys.argv[0]


def get_slurm_cpus() -> int:
    return int(os.environ.get("SLURM_CPUS_ON_NODE", 1))


def load_cfg(path: Path) -> dict:
    with open(path, "r") as f:
        return yaml.safe_load(f)


def init_zarr_store(
    zarr_store: str,
    dates: pd.DatetimeIndex,
    var_name: str,
    target_orog: xr.DataArray,
    time_chunk: int,
    y_chunk: int,
    x_chunk: int,
    mode: str = "w",
    write_global_attrs: bool = False,
    global_title: str = "HRRR regridded to URMA grid",
    synchronizer: Optional[zarr.ProcessSynchronizer] = None,
) -> None:
    target_orog = target_orog.copy()
    shape = (len(dates),) + target_orog.shape

    if y_chunk == -1:
        y_chunk = target_orog.sizes["y"]
    if x_chunk == -1:
        x_chunk = target_orog.sizes["x"]

    data = da.full(
        shape,
        np.nan,
        chunks=(time_chunk, y_chunk, x_chunk),
        dtype="float32",
    )

    base = xr.DataArray(
        data,
        dims=("time", "y", "x"),
        coords={
            "time": dates,
            "latitude": target_orog.latitude,
            "longitude": target_orog.longitude,
        },
        name=var_name,
    )

    ds_init = base.to_dataset()

    if write_global_attrs:
        ds_init.attrs = {
            "title": global_title,
            "Conventions": "CF-1.8",
            "history": "Initialized empty Zarr store",
        }

    ds_init[var_name].attrs = {
        "_FillValue": np.nan,
        "missing_value": np.nan,
    }

    ds_init.to_zarr(
        zarr_store,
        mode=mode,
        compute=False,
        zarr_format=2,
        synchronizer=synchronizer,
    )


def ensure_initialized(
    zarr_store: str,
    full_dates: pd.DatetimeIndex,
    var_name: str,
    target_orog: xr.DataArray,
    time_chunk: int,
    y_chunk: int,
    x_chunk: int,
    synchronizer: Optional[zarr.ProcessSynchronizer] = None,
) -> None:
    if not os.path.exists(zarr_store):
        print(f"[init] Creating {zarr_store} with '{var_name}'")
        init_zarr_store(
            zarr_store,
            full_dates,
            var_name,
            target_orog,
            time_chunk,
            y_chunk,
            x_chunk,
            mode="w",
            write_global_attrs=True,
            synchronizer=synchronizer,
        )
        return

    ds_meta = xr.open_zarr(zarr_store, consolidated=False, synchronizer=synchronizer)
    if "time" not in ds_meta.coords:
        raise ValueError("Zarr store missing 'time' coordinate.")

    same_len = ds_meta.sizes.get("time", -1) == full_dates.size
    same_vals = np.array_equal(
        pd.to_datetime(ds_meta.time.values),
        pd.to_datetime(full_dates.values),
    )
    if not (same_len and same_vals):
        raise ValueError("Time coordinate mismatch. Rebuild Zarr store.")

    if var_name not in ds_meta.data_vars:
        print(f"[init] Adding '{var_name}' to {zarr_store}")
        init_zarr_store(
            zarr_store,
            full_dates,
            var_name,
            target_orog,
            time_chunk,
            y_chunk,
            x_chunk,
            mode="a",
            write_global_attrs=False,
            synchronizer=synchronizer,
        )
    else:
        print(f"[init] '{var_name}' already exists in {zarr_store}")


def write_chunk(
    ds: xr.Dataset,
    zarr_store: str,
    region: Dict[str, slice],
    synchronizer: Optional[zarr.ProcessSynchronizer] = None,
) -> None:
    # Region writes only allow variables sharing the region dims (time here).
    # Keep geospatial coords in the initialized store metadata; drop them per write.
    write_ds = ds.drop_vars(["latitude", "longitude"], errors="ignore")
    write_ds = write_ds.assign_coords(time=ds.time)
    write_ds = write_ds.chunk(
        {
            "time": min(TIME_CHUNK, write_ds.sizes["time"]),
            "y": write_ds.sizes["y"],
            "x": write_ds.sizes["x"],
        }
    )
    write_ds.to_zarr(
        zarr_store,
        mode="r+",
        region=region,
        compute=True,
        align_chunks=True,
        zarr_format=2,
        synchronizer=synchronizer,
    )


def month_is_complete(
    zarr_store: str,
    var_name: str,
    month_times: pd.DatetimeIndex,
    synchronizer: Optional[zarr.ProcessSynchronizer] = None,
) -> bool:
    ds = xr.open_zarr(zarr_store, consolidated=False, synchronizer=synchronizer)
    if var_name not in ds.data_vars:
        return False
    try:
        sel = ds[var_name].sel(time=month_times)
    except KeyError:
        return False
    reduce_dims = tuple(dim for dim in sel.dims if dim != "time")
    if reduce_dims:
        per_time_has_data = sel.notnull().any(dim=reduce_dims)
    else:
        per_time_has_data = sel.notnull()
    return bool(per_time_has_data.all().compute())


def regrid_and_write_month(
    var_name: str,
    target_month: pd.Timestamp,
    full_times: pd.DatetimeIndex,
    src_zarr: str,
    regridders: RegridderRegistry,
    zarr_store: str,
    to_hr: bool,
    synchronizer: Optional[zarr.ProcessSynchronizer] = None,
) -> None:
    month_times = full_times[(full_times.year == target_month.year) & (full_times.month == target_month.month)]
    if month_times.size == 0:
        return

    if month_is_complete(zarr_store, var_name, month_times, synchronizer=synchronizer):
        print(f"[skip] {target_month.strftime('%Y%m')} already complete for {var_name}")
        return

    start_idx = int(full_times.searchsorted(month_times[0]))
    end_idx = int(full_times.searchsorted(month_times[-1]))
    src_ds = xr.open_zarr(src_zarr, consolidated=False)
    data = src_ds[var_name].isel(time=slice(start_idx, end_idx + 1))

    if to_hr:
        out = regridders.hrrr_to_urma_hr(data)
    else:
        out = regridders.hrrr_to_urma_lr_intended(data)

    out = out.astype(np.float32)
    ds_out = out.to_dataset(name=var_name)

    region = {"time": slice(start_idx, end_idx + 1)}
    write_chunk(ds_out, zarr_store, region, synchronizer=synchronizer)
    print(f"[write] {target_month.strftime('%Y%m')} {var_name} -> {region}")


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Regrid HRRR NYS Zarr to URMA HR/LR grids."
    )
    parser.add_argument(
        "--var_name",
        type=str,
        default="u10" if is_interactive() else None,
        help="Variable name to regrid (must exist in source Zarr)",
    )
    parser.add_argument(
        "--start-yearmonth",
        type=str,
        default="202301" if is_interactive() else None,
        help="Start year-month in YYYYMM (e.g., 202301)",
    )
    parser.add_argument(
        "--end-yearmonth",
        type=str,
        default="202301" if is_interactive() else None,
        help="End year-month in YYYYMM (e.g., 202301)",
    )
    parser.add_argument("--full-start-year", type=int, default=2010)
    parser.add_argument("--full-end-year", type=int, default=2040)
    parser.add_argument(
        "--method",
        type=str,
        default="bilinear" if is_interactive() else None,
        choices=["bilinear", "nearest_s2d", "patch", "conservative", "conservative_normed"],
        help="Regridding method",
    )
    parser.add_argument(
        "--intended_LR_data",
        type=str,
        default=None,
        help="If set, produce LR output for this target; if None, produce HR output.",
    )

    args = parser.parse_args() if not is_interactive() else parser.parse_known_args()[0]

    cfg = load_cfg(CFG_PATH)
    method = args.method or cfg["regridding"].get("method", "bilinear")
    intended_lr = args.intended_LR_data
    if isinstance(intended_lr, str) and intended_lr.strip().lower() in {"none", "null", ""}:
        intended_lr = None
    if intended_lr is not None and intended_lr not in {"ERA5", "EDDE", "ICON"}:
        raise ValueError("intended_LR_data must be one of ERA5, EDDE, ICON, or None.")
    cfg["regridding"]["method"] = method
    if intended_lr is not None:
        cfg["data"]["intended_LR_data"] = intended_lr

    regridders = RegridderRegistry(cfg)

    full_times = pd.date_range(
        f"{args.full_start_year}-01-01T00",
        f"{args.full_end_year}-12-31T23",
        freq="h",
    )

    if intended_lr is None:
        output_zarr = f"{OUTPUT_ROOT}/{SOURCE_NAME}_to_URMA_HR_{method}.zarr"
        target_orog = regridders.urma
        to_hr = True
    else:
        output_zarr = f"{OUTPUT_ROOT}/{SOURCE_NAME}_to_{intended_lr}_LR_{method}.zarr"
        target_orog = regridders.urma_lr_intended
        to_hr = False

    synchronizer = zarr.ProcessSynchronizer(f"{output_zarr}.sync")

    ensure_initialized(
        output_zarr,
        full_times,
        args.var_name,
        target_orog,
        TIME_CHUNK,
        Y_CHUNK,
        X_CHUNK,
        synchronizer=synchronizer,
    )

    start_month = pd.to_datetime(args.start_yearmonth, format="%Y%m")
    end_month = pd.to_datetime(args.end_yearmonth, format="%Y%m")
    months = [
        p.to_timestamp()
        for p in pd.period_range(start=start_month, end=end_month, freq="M")
    ]

    cpus = get_slurm_cpus()
    Parallel(n_jobs=cpus, backend="threading", verbose=0)(
        delayed(regrid_and_write_month)(
            args.var_name,
            m,
            full_times,
            INPUT_ZARR,
            regridders,
            output_zarr,
            to_hr=to_hr,
            synchronizer=synchronizer,
        )
        for m in tqdm(months, desc=f"{args.var_name} {args.start_yearmonth}-{args.end_yearmonth}")
    )


if __name__ == "__main__":
    main()
