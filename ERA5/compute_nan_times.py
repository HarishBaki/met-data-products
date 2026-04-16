# %%
import xarray as xr
import os

ZARR_STORE = "/network/rit/lab/basulab/Projects/DFS/DATA/ERA5_NYS/ERA5_analysis_NYS.zarr"
OUTPUT_DIR = "/network/rit/lab/basulab/Projects/DFS/DATA/ERA5_NYS/nan_times"

os.makedirs(OUTPUT_DIR, exist_ok=True)

DEFAULT_START = "2015-01-01"
DEFAULT_END = "2025-12-31"

print(f"Opening Zarr store: {ZARR_STORE}")
ds = xr.open_zarr(ZARR_STORE, chunks="auto")

print(f"Selecting time range {DEFAULT_START} to {DEFAULT_END}")
ds = ds.sel(time=slice(DEFAULT_START, DEFAULT_END))

# %%
time_year_counts = ds["time"].groupby("time.year").count().compute()
if time_year_counts.size == 0:
    print("Time coverage by year: none (no timestamps in range)")
else:
    print("Time coverage by year:")
    for year, count in zip(time_year_counts["year"].values, time_year_counts.values):
        print(f"  {int(year)}: {int(count)}")

print("Computing NaN mask for all variables...")
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
    nan_times.to_netcdf(f"{OUTPUT_DIR}/nan_times_{var}.nc")
    print(f"[done] Missing timestamps saved → {OUTPUT_DIR}/nan_times_{var}.nc")
print("[done]")

# %%
