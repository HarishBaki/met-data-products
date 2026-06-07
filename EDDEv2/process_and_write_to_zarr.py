# %%
"""
Process EDDE_V2 hourly files into a single Zarr store, one variable per month.
- Source root folder: /network/rit/lab/basulab/RAW_DATA/EDDE_V2/hourly/WRF-MPI
- Target root folder: /network/rit/lab/basulab/Projects/DFS/DATA/EDDEv2_NYS/hourly/WRF-MPI
- configurable runs and corresponding hourly time axis:
    - Historical: 1985-2014
    - SSP2-4.5: 2025-2100
    - SSP3-7.0: 2025-2100
- variables: si10, t2m, sp, u10, v10, d2m, wdir10, tp
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

RAW_ROOT = Path("/network/rit/lab/basulab/RAW_DATA/EDDE_V2/hourly/WRF-MPI")
OUTPUT_ROOT = Path("/network/rit/lab/basulab/Projects/DFS/DATA/EDDEv2_NYS/hourly/WRF-MPI")

FILE_TO_SOURCE = {
    "pr": "PRECIP",
    "ps": "PSFC",
    "td": "DEWPT",
    "ts": "T2",
    "wdirs": "WDIR10",
    "wspds": "WSPD10",
    "ua": "UE10",
    "va": "VE10",
}

SOURCE_TO_FILE = {v: k for k, v in FILE_TO_SOURCE.items()}

rename_map = {
    "PSFC": "sp",
    "T2": "t2m",
    "DEWPT": "d2m",
    "PRECIP": "tp",
    "WSPD10": "si10",
    "WDIR10": "wdir10",
    "UE10": "u10",
    "VE10": "v10",
}

VAR_NAME_TO_SOURCE = {v: k for k, v in rename_map.items()}

full_years_range = {
    "Historical": (1985, 2014),
    "SSP2-4.5": (2025, 2100),
    "SSP3-7.0": (2025, 2100),
}

ALL_VARS = list(rename_map.values())

TIME_CHUNK = 24
BATCH_SIZE = 64
y_min, y_max = 231, 343
x_min, x_max = 339, 453


def find_reference_file_for_var(var_name: str, run_type: str) -> Path:
    source_var = VAR_NAME_TO_SOURCE[var_name]
    if source_var not in SOURCE_TO_FILE:
        raise KeyError(f"Missing file prefix for source var: {source_var}")
    file_prefix = SOURCE_TO_FILE[source_var]
    run_root = run_folder(run_type)
    candidates = sorted(
        glob.glob(str(run_root / "*" / f"{file_prefix}*.nc"))
    )
    if not candidates:
        raise FileNotFoundError(f"No nc files found for {var_name} under {RAW_ROOT}")
    return Path(candidates[0])


def crop_region(ds: xr.Dataset) -> xr.Dataset:
    return ds.isel(y=slice(y_min, y_max), x=slice(x_min, x_max))


def run_folder(run_type: str) -> Path:
    years = full_years_range[run_type]
    return RAW_ROOT / run_type / f"{years[0]}-{years[1]}"


def month_from_filename(path: Path) -> pd.Timestamp:
    return pd.to_datetime(path.name.split(".")[-3], format="%Y-%m")


def files_for_month(run_type: str, target_month: pd.Timestamp, var_name: str) -> List[str]:
    source_var = VAR_NAME_TO_SOURCE[var_name]
    file_prefix = SOURCE_TO_FILE[source_var]
    year_folder = run_folder(run_type) / f"{target_month.year}"
    pattern = f"{file_prefix}.*.{target_month.strftime('%Y-%m')}.raw.nc"
    return sorted(glob.glob(str(year_folder / pattern)))


def process_single_month(
    target_month: pd.Timestamp,
    renamed_var: str,
    run_type: str,
) -> Optional[xr.Dataset]:
    files = files_for_month(run_type, target_month, renamed_var)
    if not files:
        print(f"[skip] no files for {target_month.strftime('%Y-%m')} ({run_type})")
        return None

    ds = xr.open_mfdataset(
        files,
        engine="netcdf4",
        combine="by_coords",
        parallel=False,
        autoclose=True,
    )
    ds = ds.assign_coords(
        latitude=(("y", "x"), ds["lat"].values),
        longitude=(("y", "x"), ds["lon"].values),
    ).drop_vars(["lat", "lon"], errors="ignore")
    ds = ds.assign_coords(longitude=((ds.longitude + 360) % 360))
    ds = ds.drop_vars(["mtime", "y", "x"], errors="ignore")
    ds = ds.rename({k: v for k, v in rename_map.items() if k in ds.data_vars})
    ds = crop_region(ds)
    ds = ds.transpose("time", "y", "x")
    return ds


def write_one_time(
    target_month: pd.Timestamp,
    var_name: str,
    full_times: pd.DatetimeIndex,
    run_type: str,
    output_zarr: str,
    zarr_sync: zarr.ProcessSynchronizer,
):
    ds = process_single_month(target_month, var_name, run_type)
    if ds is None:
        return

    ds = ds.sortby("time")
    times = pd.DatetimeIndex(ds.time.values)
    if not has_missing_data(output_zarr, times, var_name, zarr_sync):
        print(f"[skip] {target_month.strftime('%Y%m')} already complete in {output_zarr} for {var_name}")
        return

    ds = apply_var_attrs(ds, var_name)
    chunks = {"time": TIME_CHUNK, "y": ds.sizes["y"], "x": ds.sizes["x"]}
    write_region(ds[[var_name]], output_zarr, full_times, chunks, zarr_sync)
    print(f"[write] {target_month.strftime('%Y%m')} -> {var_name}")


# %%
if __name__ == "__main__":
    # %%
    parser = argparse.ArgumentParser(description="Process EDDEv2 data to Zarr.")
    parser.add_argument("--run-type", type=str, default="SSP2-4.5",
                        help="Run type: Historical, SSP2-4.5, SSP3-7.0")
    parser.add_argument("--var-name", type=str, default="si10",
                        choices=ALL_VARS,
                        help="Variable to process (one at a time).")
    parser.add_argument("--process-start", default="2025-01",
                        help="Start month (inclusive), e.g., 2025-01")
    parser.add_argument("--process-end", default="2030-12",
                        help="End month (inclusive), e.g., 2030-12")
    args, _ = parser.parse_known_args()

    OUTPUT_ZARR = str(OUTPUT_ROOT / f"{args.run_type}.zarr")
    ZARR_SYNC_PATH = f"{OUTPUT_ZARR}.sync"
    ZARR_SYNC = zarr.ProcessSynchronizer(ZARR_SYNC_PATH)

    full_times = pd.date_range(
        f"{full_years_range[args.run_type][0]}-01-01T00",
        f"{full_years_range[args.run_type][1]}-12-31T23",
        freq="1h",
    )
    dates = pd.date_range(start=args.process_start, end=args.process_end, freq="MS")
    var_name = args.var_name

    # %%
    def _get_template():
        ref_file = find_reference_file_for_var(var_name, args.run_type)
        ref_month = month_from_filename(ref_file)
        tmpl = process_single_month(ref_month, var_name, args.run_type)
        if tmpl is None:
            raise RuntimeError("Failed to build template dataset for initialization.")
        return apply_var_attrs(tmpl, var_name)

    chunks = {"time": TIME_CHUNK}
    ensure_store(
        OUTPUT_ZARR, full_times, var_name, _get_template, chunks,
        global_title="EDDEv2 hourly NYS subset",
        synchronizer=ZARR_SYNC,
    )

    cpus = get_slurm_cpus()
    for i in tqdm(range(0, len(dates), BATCH_SIZE), desc="EDDEv2->Zarr"):
        chunk_times = dates[i: i + BATCH_SIZE]
        Parallel(n_jobs=cpus, backend="loky")(
            delayed(write_one_time)(
                ts, var_name, full_times, args.run_type, OUTPUT_ZARR, ZARR_SYNC
            )
            for ts in chunk_times
        )

# %%
