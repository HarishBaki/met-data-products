# %%
import os
from pathlib import Path

import xarray as xr

OUTPUT_ROOT = Path("/network/rit/lab/basulab/Projects/DFS/DATA/EDDEv2_NYS/hourly/WRF-MPI")

RUN_TYPES = ["Historical", 
             "SSP2-4.5", 
             "SSP3-7.0"]
FULL_YEARS_RANGE = {
    "Historical": (1985, 2014),
    "SSP2-4.5": (2025, 2100),
    "SSP3-7.0": (2025, 2100),
}

# %%
for run_type in RUN_TYPES:
    zarr_store = OUTPUT_ROOT / f"{run_type}.zarr"
    output_dir = OUTPUT_ROOT / f"nan_times_{run_type}"
    os.makedirs(output_dir, exist_ok=True)

    start_year, end_year = FULL_YEARS_RANGE[run_type]
    default_start = f"{start_year}-01-01"
    default_end = f"{end_year}-12-31"

    print(f"Opening Zarr store: {zarr_store}")
    ds = xr.open_zarr(str(zarr_store), chunks="auto")

    print(f"Selecting time range {default_start} to {default_end}")
    ds = ds.sel(time=slice(default_start, default_end))

    time_year_counts = ds["time"].groupby("time.year").count().compute()
    if time_year_counts.size == 0:
        print("Time coverage by year: none (no timestamps in range)")
    else:
        print("Time coverage by year:")
        for year, count in zip(time_year_counts["year"].values, time_year_counts.values):
            print(f"  {int(year)}: {int(count)}")

    print(f"Computing NaN mask for all variables ({run_type})...")
    var_names = list(ds.data_vars)
    for var in var_names:
        da = ds[var]
        reduce_dims = tuple(dim for dim in da.dims if dim != "time")
        nan_mask = da.isnull().any(dim=reduce_dims)
        print(f"Collecting NaN timestamps for {var}...")
        nan_times = ds["time"].where(nan_mask).dropna("time").compute()
        print("Length of NaN timestamps:", len(nan_times))
        if nan_times.size == 0:
            print("NaN counts by year: none (no NaN timestamps)")
        else:
            year_counts = nan_times.groupby("time.year").count()
            print("NaN counts by year:")
            for year, count in zip(year_counts["year"].values, year_counts.values):
                print(f"  {int(year)}: {int(count)}")
        output_path = output_dir / f"nan_times_{var}.nc"
        nan_times.to_netcdf(str(output_path))
        print(f"[done] Missing timestamps saved → {output_path}")
    print(f"[done] {run_type}")

# %%
