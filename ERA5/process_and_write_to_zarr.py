# %%
"""
Process ERA5 hourly files into a single Zarr store, one variable per month.
- Writes to /network/rit/lab/basulab/Projects/DFS/DATA/ERA5_NYS/ERA5_analysis_NYS.zarr
- Time axis: hourly, configurable full range.
- ERA5 tp is stored in meters in source files; apply_var_attrs converts ×1000 → kg m**-2.
"""

import glob
import os
import sys
from pathlib import Path
from typing import Dict, List, Optional

import argparse
import time
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

RAW_ROOT = Path("/network/rit/lab/basulab/RAW_DATA/ERA5")
types = ["e5.oper.an.sfc", "e5.oper.fc.sfc.accumu", "e5.oper.fc.sfc.instan"]
OUTPUT_ZARR = "/network/rit/lab/basulab/Projects/DFS/DATA/ERA5_NYS/ERA5_analysis_NYS.zarr"
ZARR_SYNC_PATH = f"{OUTPUT_ZARR}.sync"
ZARR_SYNC = zarr.ProcessSynchronizer(ZARR_SYNC_PATH)

source_var_codes = {
    "128_134_sp": "SP",
    "128_165_10u": "VAR_10U",
    "128_166_10v": "VAR_10V",
    "128_167_2t": "VAR_2T",
    "128_168_2d": "VAR_2D",
    "128_142_lsp": "LSP",
    "128_143_cp": "CP",
    "228_029_i10fg": "I10FG",
}

rename_map = {
    "SP": "sp",
    "VAR_10U": "u10",
    "VAR_10V": "v10",
    "VAR_2T": "t2m",
    "VAR_2D": "d2m",
    "LSP": "lsp",
    "CP": "cp",
    "I10FG": "i10fg",
}

TYPE_TO_VAR_CODES = {
    "e5.oper.an.sfc": ["128_134_sp", "128_165_10u", "128_166_10v", "128_167_2t", "128_168_2d"],
    "e5.oper.fc.sfc.accumu": ["128_142_lsp", "128_143_cp"],
    "e5.oper.fc.sfc.instan": ["228_029_i10fg"],
}

TYPE_TO_SOURCE_VARS = {
    t: [source_var_codes[code] for code in codes]
    for t, codes in TYPE_TO_VAR_CODES.items()
}

TYPE_TO_RENAME = {
    t: {source_var_codes[code]: rename_map[source_var_codes[code]] for code in codes}
    for t, codes in TYPE_TO_VAR_CODES.items()
}

DERIVED_VARS = {
    "tp": ["lsp", "cp"],
    "si10": ["u10", "v10"],
    "wdir10": ["u10", "v10"],
}

RENAMED_TO_SOURCE = {v: k for k, v in rename_map.items()}
SOURCE_TO_CODE = {v: k for k, v in source_var_codes.items()}
CODE_TO_TYPE = {
    code: data_type
    for data_type, codes in TYPE_TO_VAR_CODES.items()
    for code in codes
}

ALL_VARS = list(rename_map.values()) + list(DERIVED_VARS.keys())

# ERA5 source units that may be absent from file attrs.
# tp (lsp+cp) is in meters in ERA5 — apply_var_attrs converts ×1000 → kg m**-2.
_SOURCE_UNITS_FALLBACK = {"tp": "m"}

TIME_CHUNK = 24
BATCH_SIZE = 64
LAT_MIN = 38
LAT_MAX = 48
LON_MIN = -82
LON_MAX = -68


def find_reference_file_for_var(var_name: str) -> Path:
    if var_name in DERIVED_VARS:
        var_name = DERIVED_VARS[var_name][0]
    if var_name not in RENAMED_TO_SOURCE:
        raise KeyError(f"Unknown variable: {var_name}")
    source_var = RENAMED_TO_SOURCE[var_name]
    var_code = SOURCE_TO_CODE[source_var]
    data_type = CODE_TO_TYPE[var_code]
    candidates = sorted(
        glob.glob(str(RAW_ROOT / data_type / "*" / f"*{var_code}*.nc"))
    )
    if not candidates:
        raise FileNotFoundError(f"No nc files found for {var_name} under {RAW_ROOT}")
    return Path(candidates[0])


def crop_region(ds: xr.Dataset) -> xr.Dataset:
    if float(ds.longitude.max()) > 180:
        lon_min = LON_MIN + 360 if LON_MIN < 0 else LON_MIN
        lon_max = LON_MAX + 360 if LON_MAX < 0 else LON_MAX
    else:
        lon_min, lon_max = LON_MIN, LON_MAX
    lat_slice = slice(LAT_MAX, LAT_MIN) if ds.latitude[0] > ds.latitude[-1] else slice(LAT_MIN, LAT_MAX)
    lon_slice = slice(lon_min, lon_max) if ds.longitude[0] < ds.longitude[-1] else slice(lon_max, lon_min)
    return ds.sel(latitude=lat_slice, longitude=lon_slice)


def forecast_to_valid_time(ds: xr.Dataset) -> xr.Dataset:
    fi = np.asarray(ds.coords["forecast_initial_time"].values)
    fh = np.asarray(ds.coords["forecast_hour"].values)
    valid_time_2d = fi[None, :] + fh[:, None].astype("timedelta64[h]")
    valid_time_da = xr.DataArray(
        valid_time_2d,
        coords={"forecast_hour": ds.forecast_hour, "forecast_initial_time": ds.forecast_initial_time},
        dims=("forecast_hour", "forecast_initial_time"),
    )
    valid_time_flat = valid_time_da.stack(time=("forecast_hour", "forecast_initial_time"))
    ds = ds.stack(time=("forecast_hour", "forecast_initial_time"))
    ds = ds.assign_coords(valid_time=valid_time_flat)
    ds = ds.swap_dims({"time": "valid_time"})
    ds = ds.drop_vars(["forecast_hour", "forecast_initial_time", "time"])
    ds = ds.sortby("valid_time")
    return ds.rename(valid_time="time")


def expand_requested_vars(requested_vars: List[str]) -> List[str]:
    expanded: List[str] = []
    for var in requested_vars:
        if var in DERIVED_VARS:
            for dep in DERIVED_VARS[var]:
                if dep not in expanded:
                    expanded.append(dep)
        if var not in expanded:
            expanded.append(var)
    return expanded


def vars_to_type_chain(vars_in: List[str]) -> Dict[str, Dict[str, str]]:
    mapping: Dict[str, Dict[str, str]] = {}
    for var in vars_in:
        if var in DERIVED_VARS:
            continue
        source = RENAMED_TO_SOURCE[var]
        code = SOURCE_TO_CODE[source]
        data_type = CODE_TO_TYPE[code]
        mapping[var] = {"source_var": source, "var_code": code, "type": data_type}
    return mapping


def files_for_type_month(
    target_month: pd.Timestamp,
    data_type: str,
    var_codes: Optional[List[str]] = None,
) -> List[str]:
    folder = f"{RAW_ROOT}/{data_type}/{target_month.strftime('%Y%m')}"
    codes = var_codes if var_codes is not None else TYPE_TO_VAR_CODES.get(data_type, [])
    if not codes:
        return sorted(glob.glob(f"{folder}/*.nc"))
    files: List[str] = []
    for code in codes:
        files.extend(glob.glob(f"{folder}/*{code}*.nc"))
    return sorted(set(files))


def apply_requested_vars(
    ds: xr.Dataset,
    requested_vars: Optional[List[str]],
    expanded_vars: Optional[List[str]],
    allow_derived: bool,
) -> xr.Dataset:
    rename = {k: v for k, v in rename_map.items() if k in ds.data_vars}
    ds = ds.rename(rename)
    if not requested_vars or not expanded_vars:
        return ds
    wanted = [v for v in ds.data_vars if v in expanded_vars]
    ds = ds[wanted]
    if allow_derived:
        for var in requested_vars:
            if var not in DERIVED_VARS:
                continue
            missing = [v for v in DERIVED_VARS[var] if v not in ds.data_vars]
            if missing:
                raise KeyError(f"Missing inputs for {var}: {missing}")
            if var == "tp":
                ds["tp"] = ds["lsp"] + ds["cp"]
            elif var == "si10":
                ds["si10"] = np.sqrt(ds["u10"] ** 2 + ds["v10"] ** 2)
            elif var == "wdir10":
                ds["wdir10"] = (
                    (270 - np.rad2deg(np.arctan2(ds["v10"], ds["u10"]))) % 360
                ).where((ds["u10"] != 0) | (ds["v10"] != 0), other=0)
            for dep in DERIVED_VARS[var]:
                if dep not in requested_vars and dep in ds.data_vars:
                    ds = ds.drop_vars(dep)
    return ds


def process_single_month(
    target_month: pd.Timestamp,
    renamed_var: str,
) -> Optional[xr.Dataset]:
    expanded_vars = expand_requested_vars([renamed_var])
    chain = vars_to_type_chain(expanded_vars)
    data_types = {info["type"] for info in chain.values()}
    if len(data_types) != 1:
        raise ValueError(f"Multiple data types required for {renamed_var}: {sorted(data_types)}")
    data_type = data_types.pop()
    var_codes = [info["var_code"] for info in chain.values()]

    files = files_for_type_month(target_month, data_type, var_codes)
    if not files:
        print(f"[skip] no files for {target_month.strftime('%Y%m')} ({data_type})")
        return None
    ds = xr.open_mfdataset(
        files,
        engine="netcdf4",
        combine="by_coords",
        parallel=False,
        autoclose=True,
    )
    if data_type != types[0]:
        ds = forecast_to_valid_time(ds)

    ds = apply_requested_vars(
        ds,
        [renamed_var],
        expanded_vars,
        allow_derived=renamed_var in DERIVED_VARS,
    )
    ds = crop_region(ds)
    ds = ds.transpose("time", "latitude", "longitude")
    return ds


def write_one_time(
    target_month: pd.Timestamp,
    var_name: str,
    full_times: pd.DatetimeIndex,
):
    ds = process_single_month(target_month, var_name)
    if ds is None:
        return

    ds = ds.sortby("time")
    times = pd.DatetimeIndex(ds.time.values)
    if not has_missing_data(OUTPUT_ZARR, times, var_name):
        print(f"[skip] {target_month.strftime('%Y%m')} already complete in {OUTPUT_ZARR} for {var_name}")
        return

    # Ensure src units are set for derived variables (e.g., tp from lsp+cp in meters).
    if not ds[var_name].attrs.get("units") and var_name in _SOURCE_UNITS_FALLBACK:
        ds[var_name].attrs["units"] = _SOURCE_UNITS_FALLBACK[var_name]
    ds = apply_var_attrs(ds, var_name)

    chunks = {"time": TIME_CHUNK, "latitude": ds.sizes["latitude"], "longitude": ds.sizes["longitude"]}
    write_region(ds[[var_name]], OUTPUT_ZARR, full_times, chunks, ZARR_SYNC)
    print(f"[write] {target_month.strftime('%Y%m')} -> {var_name}")


# %%
if __name__ == "__main__":
    # %%
    parser = argparse.ArgumentParser(description="Process ERA5 data to Zarr.")
    parser.add_argument(
        "--var-name",
        type=str,
        default="si10",
        choices=sorted(set(rename_map.values()) | set(DERIVED_VARS.keys())),
        help="Variable to process (one at a time).",
    )
    parser.add_argument("--process-start", default="2017-12",
                        help="Start month (inclusive), e.g., 2017-12")
    parser.add_argument("--process-end", default="2025-12",
                        help="End month (inclusive), e.g., 2025-12")
    parser.add_argument("--full-start-year", type=int, default=2015)
    parser.add_argument("--full-end-year", type=int, default=2030)
    args, _ = parser.parse_known_args()

    full_times = pd.date_range(
        f"{args.full_start_year}-01-01T00",
        f"{args.full_end_year}-12-31T23",
        freq="1h",
    )
    dates = pd.date_range(start=args.process_start, end=args.process_end, freq="MS")
    var_name = args.var_name

    # %%
    def _get_template():
        ref_file = find_reference_file_for_var(var_name)
        ref_month = pd.to_datetime(ref_file.parent.name, format="%Y%m")
        tmpl = process_single_month(ref_month, var_name)
        if tmpl is None:
            raise RuntimeError("Failed to build template dataset for initialization.")
        if not tmpl[var_name].attrs.get("units") and var_name in _SOURCE_UNITS_FALLBACK:
            tmpl[var_name].attrs["units"] = _SOURCE_UNITS_FALLBACK[var_name]
        return apply_var_attrs(tmpl, var_name)

    chunks = {"time": TIME_CHUNK}
    ensure_store(
        OUTPUT_ZARR, full_times, var_name, _get_template, chunks,
        global_title="ERA5 hourly NYS subset",
        synchronizer=ZARR_SYNC,
    )

    cpus = get_slurm_cpus()
    for i in tqdm(range(0, len(dates), BATCH_SIZE), desc="ERA5->Zarr"):
        chunk_times = dates[i: i + BATCH_SIZE]
        Parallel(n_jobs=cpus, backend="loky")(
            delayed(write_one_time)(ts, var_name, full_times) for ts in chunk_times
        )

# %%
