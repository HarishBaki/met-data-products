# %%
import xarray as xr
import zarr
project_dir = '/network/rit/lab/basulab/Harish/DFS'
zarr_store = '/network/rit/lab/basulab/Projects/DFS/DATA/URMA_NYS/URMA_NYS.zarr'
output_dir = '/network/rit/lab/basulab/Projects/DFS/DATA/URMA_NYS/nan_times'
ds = xr.open_zarr(zarr_store)
# %%
var_names = list(ds.data_vars)
for var_name in var_names:
    print(var_name)
    nan_mask = ds[var_name].isnull().any(dim=("y", "x"))

    nan_times = ds["time"].where(nan_mask).sel(time=slice("2014-01-01", "2025-12-31")).dropna("time").compute()

    nan_times.to_netcdf(f"{output_dir}/nan_times_{var_name}.nc")
    print(f"[done] Missing timestamps saved → {output_dir}/nan_times_{var_name}.nc")