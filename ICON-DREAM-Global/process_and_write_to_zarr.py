# %%
"""
Process ICON-DREAM-Global hourly GRIB files into a single Zarr store.
- Source root folder: /network/rit/lab/basulab/RAW_DATA/ICON-DREAM-Global
- Target store: /network/rit/lab/basulab/Projects/DFS/DATA/ICON_DREAM_Global_NYS/ICON_DREAM_Global_NYS.zarr
- Time axis: hourly, configurable full range.
- Variables: si10, i10fg, t2m, sp, d2m, u10, v10, wdir10, tp, fsr
"""

import argparse
import glob
import os
import sys
import time
from pathlib import Path
from typing import Dict, List, Optional

import dask.array as da
import numpy as np
import pandas as pd
import xarray as xr
import zarr
from joblib import Parallel, delayed
from tqdm import tqdm

BOOTSTRAP_ROOT = Path(__file__).resolve().parents[1]
if str(BOOTSTRAP_ROOT) not in sys.path:
    sys.path.insert(0, str(BOOTSTRAP_ROOT))

from data_utils.zarr_io import (
    apply_var_attrs,
    ensure_store,
    get_slurm_cpus,
    has_missing_data,
    open_zarr_safe,
    write_region,
)

RAW_ROOT = Path("/network/rit/lab/basulab/RAW_DATA/ICON-DREAM-Global")
OUTPUT_ROOT = Path("/network/rit/lab/basulab/Projects/DFS/DATA/ICON_DREAM_Global_NYS")
OUTPUT_ZARR = OUTPUT_ROOT / "ICON_DREAM_Global_NYS.zarr"
ZARR_SYNC_PATH = f"{OUTPUT_ZARR}.sync"
ZARR_SYNC = zarr.ProcessSynchronizer(ZARR_SYNC_PATH)
MASK_PATH = OUTPUT_ROOT / "icon_global_nys_mask.nc"

FILE_TO_SOURCE = {
    "TD_2M": "d2m",
    "TOT_PREC": "tp",
    "T_2M": "t2m",
    "WS_10M": "si10",
    "PS": "sp",
    "U_10M": "u10",
    "V_10M": "v10",
    "VMAX_10M": "fg10",
    "Z0": "fsr",
}

rename_map = {
    "d2m": "d2m",
    "tp": "tp",
    "t2m": "t2m",
    "si10": "si10",
    "sp": "sp",
    "u10": "u10",
    "v10": "v10",
    "fg10": "i10fg",
    "fsr": "fsr",
}

SOURCE_TO_FILE = {v: k for k, v in FILE_TO_SOURCE.items()}
VAR_NAME_TO_SOURCE = {v: k for k, v in rename_map.items()}

DERIVED_VARS: Dict[str, List[str]] = {
    "wdir10": ["u10", "v10"],
}

ALL_VARS = list(rename_map.values()) + list(DERIVED_VARS.keys())

TIME_CHUNK = 24
BATCH_SIZE = 12


def is_interactive() -> bool:
    import __main__ as main
    return not hasattr(main, "__file__") or "ipykernel" in sys.argv[0]


def load_mask() -> xr.Dataset:
    if not MASK_PATH.exists():
        raise FileNotFoundError(f"Mask not found: {MASK_PATH}")
    return xr.open_dataset(MASK_PATH)


def stash_scalar_var_as_attr(ds: xr.Dataset, name: str) -> xr.Dataset:
    if name not in ds:
        return ds
    val = ds[name].values
    if np.ndim(val) == 0 or (hasattr(val, "size") and val.size == 1):
        if np.issubdtype(np.asarray(val).dtype, np.number):
            ds.attrs[name] = float(np.asarray(val))
        else:
            ds.attrs[name] = str(np.asarray(val))
    else:
        ds.attrs[name] = np.asarray(val).tolist()
    return ds.drop_vars(name)


def crop_to_nys(ds: xr.Dataset, mask_ds: xr.Dataset) -> xr.Dataset:
    mask = mask_ds["mask"]
    lat = mask_ds["lat"]
    lon = mask_ds["lon"]
    ds = ds.where(mask, drop=True)
    ds = ds.drop_vars(["lat", "lon"], errors="ignore")
    lat_sel = lat.where(mask, drop=True)
    lon_sel = lon.where(mask, drop=True)
    ds = ds.drop_vars(["latitude", "longitude"], errors="ignore")
    return ds.assign_coords(
        lat=("values", lat_sel.values),
        lon=("values", lon_sel.values),
    )


def normalize_time(ds: xr.Dataset, target_month: pd.Timestamp) -> xr.Dataset:
    ds = (
        ds.stack(time_step=("time", "step"))
        .swap_dims({"time_step": "valid_time"})
        .drop_vars(["step", "time", "time_step"])
        .rename({"valid_time": "time"})
    )
    ds = stash_scalar_var_as_attr(ds, "heightAboveGround")
    ds = stash_scalar_var_as_attr(ds, "surface")
    ds = ds.sel(time=target_month.strftime("%Y-%m"))
    return ds.sortby("time")


def file_for_month(shortname: str, target_month: pd.Timestamp) -> Path:
    if shortname not in SOURCE_TO_FILE:
        raise KeyError(f"Missing folder name for shortName: {shortname}")
    folder = SOURCE_TO_FILE[shortname]
    fname = f"ICON-DREAM-Global_{target_month.strftime('%Y%m')}_{folder}_hourly.grb"
    return RAW_ROOT / folder / fname


def source_attrs_for_var(var_name: str) -> Dict:
    if var_name in DERIVED_VARS:
        return {}
    if var_name not in VAR_NAME_TO_SOURCE:
        raise KeyError(f"Unknown variable: {var_name}")
    shortname = VAR_NAME_TO_SOURCE[var_name]
    folder = SOURCE_TO_FILE.get(shortname)
    if folder is None:
        return {}
    pattern = str(RAW_ROOT / folder / f"ICON-DREAM-Global_*_{folder}_hourly.grb")
    candidates = sorted(glob.glob(pattern))
    if not candidates:
        return {}
    ds = xr.open_dataset(
        candidates[0],
        engine="cfgrib",
        backend_kwargs={"indexpath": ""},
    )
    try:
        candidates_v = [v for v in ds.data_vars if v not in {"lat", "lon", "time", "mtime"}]
        data_var = candidates_v[0] if candidates_v else next(iter(ds.data_vars))
        return dict(ds[data_var].attrs)
    finally:
        ds.close()


def open_single_var(shortname: str, target_month: pd.Timestamp) -> xr.Dataset:
    path = file_for_month(shortname, target_month)
    if not path.exists():
        raise FileNotFoundError(f"Missing file: {path}")
    ds = xr.open_dataset(
        path,
        engine="cfgrib",
        backend_kwargs={"indexpath": ""},
    )
    candidates = [v for v in ds.data_vars if v not in {"lat", "lon", "time", "mtime"}]
    data_var = candidates[0] if candidates else next(iter(ds.data_vars))
    if data_var != shortname:
        ds = ds.rename({data_var: shortname})
    ds = ds.rename({k: v for k, v in rename_map.items() if k in ds.data_vars})
    ds = normalize_time(ds, target_month)
    return ds


def apply_derived(ds: xr.Dataset, requested_vars: List[str]) -> xr.Dataset:
    if "wdir10" in requested_vars:
        if "u10" not in ds.data_vars or "v10" not in ds.data_vars:
            raise KeyError("u10/v10 required for wdir10")
        ds["wdir10"] = (
            (270 - np.rad2deg(np.arctan2(ds["v10"], ds["u10"]))) % 360
        ).where((ds["u10"] != 0) | (ds["v10"] != 0), other=0)
    return ds


def process_single_month(
    target_month: pd.Timestamp,
    var_name: str,
    mask_ds: xr.Dataset,
) -> Optional[xr.Dataset]:
    try:
        if var_name in DERIVED_VARS:
            deps = DERIVED_VARS[var_name]
            parts = [open_single_var(VAR_NAME_TO_SOURCE[d], target_month) for d in deps]
            ds = xr.merge(parts, join="inner")
            ds = apply_derived(ds, [var_name])
            ds = ds[[var_name]]
        else:
            if var_name not in VAR_NAME_TO_SOURCE:
                raise KeyError(f"Unknown variable: {var_name}")
            shortname = VAR_NAME_TO_SOURCE[var_name]
            ds = open_single_var(shortname, target_month)
            if var_name not in ds.data_vars:
                return None
            ds = ds[[var_name]]
    except FileNotFoundError as exc:
        print(f"[skip] {target_month.strftime('%Y%m')} missing source file: {exc}")
        return None

    ds = crop_to_nys(ds, mask_ds)
    ds = apply_var_attrs(ds, var_name)
    ds[var_name] = ds[var_name].astype(np.float32)
    return ds


def month_has_data(zarr_store: str, var_name: str, month_times: pd.DatetimeIndex) -> bool:
    ds = open_zarr_safe(zarr_store, ZARR_SYNC)
    if var_name not in ds.data_vars:
        return False
    try:
        sel = ds[var_name].sel(time=month_times)
    except KeyError:
        return False
    return bool(sel.notnull().any().compute())


def process_and_write_month(
    var_name: str,
    target_month: pd.Timestamp,
    full_times: pd.DatetimeIndex,
    mask_ds: xr.Dataset,
    zarr_store: str,
) -> None:
    ds = process_single_month(target_month, var_name, mask_ds)
    if ds is None:
        print(f"[skip] no data for {target_month.strftime('%Y%m')} {var_name}")
        return

    month_times = full_times[
        (full_times.year == target_month.year) & (full_times.month == target_month.month)
    ]
    if month_times.size == 0:
        return

    if month_has_data(zarr_store, var_name, month_times):
        print(f"[skip] {target_month.strftime('%Y%m')} already has data for {var_name}")
        return

    ds = ds.reindex(time=month_times)
    ds = ds.reset_coords(names=["lat", "lon"], drop=True)

    # apply_var_attrs already called in process_single_month.
    # For ICON: move _FillValue out of attrs and into encoding (zarr convention).
    attrs = dict(ds[var_name].attrs)
    attrs.pop("_FillValue", None)
    attrs.pop("missing_value", None)
    ds[var_name].attrs = attrs
    ds[var_name].encoding["_FillValue"] = np.nan
    ds[var_name].encoding["missing_value"] = np.nan

    start_idx = int(full_times.searchsorted(month_times[0]))
    end_idx = int(full_times.searchsorted(month_times[-1]))
    region = {"time": slice(start_idx, end_idx + 1)}

    ds.to_zarr(
        zarr_store,
        mode="r+",
        region=region,
        compute=True,
        zarr_format=2,
        synchronizer=ZARR_SYNC,
    )
    print(f"[write] {target_month.strftime('%Y%m')} {var_name} -> {region}")


# %%
if __name__ == "__main__":
    # %%
    parser = argparse.ArgumentParser(
        description="Process monthly ICON-DREAM-Global GRIB files into a single Zarr store."
    )
    parser.add_argument(
        "--var_name",
        type=str,
        default="wdir10" if is_interactive() else None,
        choices=ALL_VARS,
        help="Standardized variable name",
    )
    parser.add_argument(
        "--start-yearmonth",
        type=str,
        default="202001" if is_interactive() else None,
        help="Start year-month in YYYYMM (e.g., 202001)",
    )
    parser.add_argument(
        "--end-yearmonth",
        type=str,
        default="202001" if is_interactive() else None,
        help="End year-month in YYYYMM (e.g., 202001)",
    )
    parser.add_argument("--full-start-year", type=int, default=2010)
    parser.add_argument("--full-end-year", type=int, default=2025)

    # %%
    if is_interactive():
        args, _ = parser.parse_known_args()
    else:
        args = parser.parse_args()

    var_name = args.var_name
    start_yearmonth = args.start_yearmonth
    end_yearmonth = args.end_yearmonth

    full_times = pd.date_range(
        f"{args.full_start_year}-01-01T00",
        f"{args.full_end_year}-12-31T23",
        freq="h",
    )

    mask_ds = load_mask()

    def _get_template():
        lat = mask_ds["lat"].where(mask_ds["mask"], drop=True)
        lon = mask_ds["lon"].where(mask_ds["mask"], drop=True)
        return xr.Dataset(coords={"lat": lat, "lon": lon})

    chunks = {"time": TIME_CHUNK}
    ensure_store(
        str(OUTPUT_ZARR),
        full_times,
        var_name,
        _get_template,
        chunks,
        global_title="ICON-DREAM-Global NYS subset (unstructured)",
        extra_var_attrs=source_attrs_for_var(var_name),
        synchronizer=ZARR_SYNC,
    )

    start_month = pd.to_datetime(start_yearmonth, format="%Y%m")
    end_month = pd.to_datetime(end_yearmonth, format="%Y%m")
    months = [
        p.to_timestamp()
        for p in pd.period_range(start=start_month, end=end_month, freq="M")
    ]
    cpus = get_slurm_cpus()
    print(cpus)
    # %%
    for i in tqdm(range(0, len(months), BATCH_SIZE), desc=f"{var_name} {start_yearmonth}-{end_yearmonth}"):
        batch = months[i: i + BATCH_SIZE]
        Parallel(n_jobs=cpus, backend="loky", verbose=0)(
            delayed(process_and_write_month)(var_name, m, full_times, mask_ds, str(OUTPUT_ZARR))
            for m in batch
        )

# %%
