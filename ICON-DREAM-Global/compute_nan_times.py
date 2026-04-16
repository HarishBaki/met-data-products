# %%
import xarray as xr
import zarr
import os
zarr_store = "/network/rit/lab/basulab/Projects/DFS/DATA/ICON_DREAM_Global_NYS/ICON_DREAM_Global_NYS.zarr"
output_dir = '/network/rit/lab/basulab/Projects/DFS/DATA/ICON_DREAM_Global_NYS/nan_times'
os.makedirs(output_dir, exist_ok=True)
ds = xr.open_zarr(zarr_store)
# %%
var_names = list(ds.data_vars)
for var_name in var_names:
    print(var_name)
    data_var = ds[var_name]
    reduce_dims = [d for d in data_var.dims if d != "time"]
    nan_mask = data_var.isnull().any(dim=reduce_dims) if reduce_dims else data_var.isnull()

    nan_times = ds["time"].where(nan_mask).dropna("time").compute()

    nan_times.to_netcdf(f"{output_dir}/nan_times_{var_name}.nc")
    print(f"[done] Missing timestamps saved → {output_dir}/nan_times_{var_name}.nc")

# %%
