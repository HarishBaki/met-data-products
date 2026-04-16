# %%
"""Shared regridding utilities used by data prep and training."""
from __future__ import annotations

from typing import Iterable
import time
from pathlib import Path

import zarr
import numpy as np
import xarray as xr
import xesmf as xe
from xesmf.backend import Mesh
from xesmf.frontend import BaseRegridder, ds_to_ESMFgrid
from scipy.ndimage import uniform_filter


# ---------------------------------------------------
# Helper: uniform filter for PP LR generation
# ---------------------------------------------------
def xr_uniform_filter(da, size, mode="reflect"):
    return xr.apply_ufunc(
        uniform_filter,
        da,
        kwargs={"size": size, "mode": mode},
        input_core_dims=[["y", "x"]],
        output_core_dims=[["y", "x"]],
        dask="parallelized",
        dask_gufunc_kwargs={"allow_rechunk": True},
        output_dtypes=[da.dtype],
    )


# ---------------------------------------------------
# Helper: add bounds for conservative regridding
# ---------------------------------------------------
def add_bounds_2d(ds, lon="longitude", lat="latitude"):
    is_dataarray = isinstance(ds, xr.DataArray)
    if is_dataarray:
        ds = ds._to_temp_dataset()
    if lon not in ds.coords and "lon" in ds.coords:
        lon = "lon"
    if lat not in ds.coords and "lat" in ds.coords:
        lat = "lat"
    lon_da = ds[lon]
    lat_da = ds[lat]
    lon2 = lon_da.values
    lat2 = lat_da.values
    if lon2.ndim == 1 and lat2.ndim == 1:
        lon_b = np.empty(lon2.size + 1, dtype=lon2.dtype)
        lat_b = np.empty(lat2.size + 1, dtype=lat2.dtype)
        lon_b[1:-1] = 0.5 * (lon2[:-1] + lon2[1:])
        lat_b[1:-1] = 0.5 * (lat2[:-1] + lat2[1:])
        lon_b[0] = lon2[0] - 0.5 * (lon2[1] - lon2[0])
        lon_b[-1] = lon2[-1] + 0.5 * (lon2[-1] - lon2[-2])
        lat_b[0] = lat2[0] - 0.5 * (lat2[1] - lat2[0])
        lat_b[-1] = lat2[-1] + 0.5 * (lat2[-1] - lat2[-2])
        ds = ds.assign_coords(
            lon_b=((lon_da.dims[0] + "_b",), lon_b),
            lat_b=((lat_da.dims[0] + "_b",), lat_b),
        )
        ds[lon].attrs = {**ds[lon].attrs, "bounds": "lon_b"}
        ds[lat].attrs = {**ds[lat].attrs, "bounds": "lat_b"}
        return ds

    ny, nx = lon2.shape

    lonb = np.empty((ny + 1, nx + 1), dtype=lon2.dtype)
    latb = np.empty((ny + 1, nx + 1), dtype=lat2.dtype)

    lonb[1:-1, 1:-1] = 0.25 * (
        lon2[:-1, :-1] + lon2[1:, :-1] + lon2[:-1, 1:] + lon2[1:, 1:]
    )
    latb[1:-1, 1:-1] = 0.25 * (
        lat2[:-1, :-1] + lat2[1:, :-1] + lat2[:-1, 1:] + lat2[1:, 1:]
    )

    lonb[0, 1:-1] = lonb[1, 1:-1] - (lonb[2, 1:-1] - lonb[1, 1:-1])
    lonb[-1, 1:-1] = lonb[-2, 1:-1] + (lonb[-2, 1:-1] - lonb[-3, 1:-1])
    lonb[1:-1, 0] = lonb[1:-1, 1] - (lonb[1:-1, 2] - lonb[1:-1, 1])
    lonb[1:-1, -1] = lonb[1:-1, -2] + (lonb[1:-1, -2] - lonb[1:-1, -3])

    latb[0, 1:-1] = latb[1, 1:-1] - (latb[2, 1:-1] - latb[1, 1:-1])
    latb[-1, 1:-1] = latb[-2, 1:-1] + (latb[-2, 1:-1] - latb[-3, 1:-1])
    latb[1:-1, 0] = latb[1:-1, 1] - (latb[1:-1, 2] - latb[1:-1, 1])
    latb[1:-1, -1] = latb[1:-1, -2] + (latb[1:-1, -2] - latb[1:-1, -3])

    lonb[0, 0] = lonb[0, 1] + lonb[1, 0] - lonb[1, 1]
    lonb[0, -1] = lonb[0, -2] + lonb[1, -1] - lonb[1, -2]
    lonb[-1, 0] = lonb[-2, 0] + lonb[-1, 1] - lonb[-2, 1]
    lonb[-1, -1] = lonb[-2, -1] + lonb[-1, -2] - lonb[-2, -2]

    latb[0, 0] = latb[0, 1] + latb[1, 0] - latb[1, 1]
    latb[0, -1] = latb[0, -2] + latb[1, -1] - latb[1, -2]
    latb[-1, 0] = latb[-2, 0] + latb[-1, 1] - latb[-2, 1]
    latb[-1, -1] = latb[-2, -1] + latb[-1, -2] - latb[-2, -2]

    ds = ds.assign_coords(
        lon_b=(("y_b", "x_b"), lonb),
        lat_b=(("y_b", "x_b"), latb),
    )
    ds[lon].attrs = {**ds[lon].attrs, "bounds": "lon_b"}
    ds[lat].attrs = {**ds[lat].attrs, "bounds": "lat_b"}
    return ds


# ---------------------------------------------------
# Regridder registry (created ONCE, shared)
# ---------------------------------------------------
class RegridderRegistry:
    """
    Builds and stores all xESMF regridders using ONLY the config dict.
    No hard-coded paths, no manual ratio passing.
    """

    def __init__(self, config: dict):
        paths_cfg = config["paths"]
        regrid_cfg = config["regridding"]
        default_method = regrid_cfg.get("method", "bilinear")
        conservative_methods = {"conservative", "conservative_normed"}

        def _maybe_add_bounds(ds, method: str):
            return add_bounds_2d(ds) if method in conservative_methods else ds

        def _infer_icon_dim(da: xr.DataArray) -> str:
            if "cell" in da.dims:
                return "cell"
            if "values" in da.dims:
                return "values"
            return da.dims[0]

        def _to_degrees(lon, lat):
            if float(np.nanmax(lon)) < 6.5:
                lon = np.rad2deg(lon)
                lat = np.rad2deg(lat)
                lon = (lon + 360) % 360
            return lon, lat

        def _build_icon_mesh():
            try:
                from shapely.geometry import Polygon
            except ImportError as exc:
                raise ImportError("shapely is required to build ICON meshes") from exc

            grid = xr.open_dataset(paths_cfg["icon_grid"])
            mask = xr.open_dataset(paths_cfg["icon_nys_mask"])["mask"].values.astype(bool)

            clon_v = grid["clon_vertices"].isel(cell=mask).values
            clat_v = grid["clat_vertices"].isel(cell=mask).values
            clon_v, clat_v = _to_degrees(clon_v, clat_v)

            polys = [
                Polygon(list(zip(clon_v[i], clat_v[i]))) for i in range(clon_v.shape[0])
            ]
            return Mesh.from_polygons(polys)

        def _fix_icon_output(out, target_ds):
            if "dummy_new" in out.dims and "values_new" in out.dims:
                out = out.rename({"dummy_new": "y", "values_new": "x"})
            if "longitude" in target_ds and "latitude" in target_ds:
                out = out.assign_coords(
                    longitude=target_ds["longitude"],
                    latitude=target_ds["latitude"],
                )
            return out

        def _wrap_icon_regridder(regridder, target_ds):
            def _call(da):
                return _fix_icon_output(regridder(da), target_ds)

            return _call

        # Load only the grids needed for xESMF
        self.urma = xr.open_dataset(paths_cfg["urma_orog"]).orog.copy()
        self.hrrr = xr.open_dataset(paths_cfg["hrrr_orog"]).orog.copy()
        self.era5 = xr.open_dataset(paths_cfg["era5_orog"]).orog.copy()
        self.edde = xr.open_dataset(paths_cfg["edde_orog"]).orog.copy()
        self.icon_global = xr.open_dataset(paths_cfg["icon_orog"]).orog.copy()

        self.method = default_method

        # Explicit resolution factors per source
        self.era5_res_y, self.era5_res_x = tuple(regrid_cfg["ERA5_resolution_factor"])
        self.edde_res_y, self.edde_res_x = tuple(regrid_cfg["EDDE_resolution_factor"])
        self.icon_res_y, self.icon_res_x = tuple(regrid_cfg["ICON_resolution_factor"])

        # HR regridders
        self.era5_to_urma_hr = xe.Regridder(
            _maybe_add_bounds(self.era5, self.method),
            _maybe_add_bounds(self.urma, self.method),
            method=self.method,
            reuse_weights=False,
        )
        self.hrrr_to_urma_hr = xe.Regridder(
            _maybe_add_bounds(self.hrrr, self.method),
            _maybe_add_bounds(self.urma, self.method),
            method=self.method,
            reuse_weights=False,
        )
        self.edde_to_urma_hr = xe.Regridder(
            _maybe_add_bounds(self.edde, self.method),
            _maybe_add_bounds(self.urma, self.method),
            method=self.method,
            reuse_weights=False,
        )

        self.icon_use_mesh = regrid_cfg.get("icon_use_mesh", True)
        self.icon_mesh = None
        self.icon_input_dim = _infer_icon_dim(self.icon_global)
        icon_mask = xr.open_dataset(paths_cfg["icon_nys_mask"])["mask"].values.astype(bool)
        if self.icon_use_mesh:
            self.icon_mesh = _build_icon_mesh()
            if self.icon_global.sizes[self.icon_input_dim] == icon_mask.size:
                self.icon_global = self.icon_global.isel({self.icon_input_dim: icon_mask})
        else:
            if self.icon_global.sizes[self.icon_input_dim] == icon_mask.size:
                self.icon_global = self.icon_global.isel({self.icon_input_dim: icon_mask})

        if self.icon_use_mesh:
            need_bounds = self.method in conservative_methods
            target_ds = _maybe_add_bounds(self.urma, self.method)
            grid_out, _, _ = ds_to_ESMFgrid(target_ds, need_bounds=need_bounds, periodic=False)
            icon_global_to_urma_hr = BaseRegridder(
                self.icon_mesh,
                grid_out,
                method=self.method,
                input_dims=(self.icon_input_dim,),
                output_dims=("y", "x"),
                unmapped_to_nan=True,
            )
            grid_out.destroy()
            self.icon_global_to_urma_hr = _wrap_icon_regridder(icon_global_to_urma_hr, target_ds)
        else:
            if self.method != "nearest_s2d":
                raise ValueError(
                    "ICON locstream regridding only supports nearest_s2d. Enable icon_use_mesh."
                )
            icon_global_to_urma_hr = xe.Regridder(
                self.icon_global,
                self.urma,
                method="nearest_s2d",
                locstream_in=True,
                reuse_weights=False,
            )
            self.icon_global_to_urma_hr = _wrap_icon_regridder(icon_global_to_urma_hr, self.urma)

        # LR regridders (targeted to intended_LR_data resolution)
        intended_lr = config["data"].get("intended_LR_data")
        if intended_lr == "ERA5":
            lr_res_y, lr_res_x = self.era5_res_y, self.era5_res_x
        elif intended_lr == "EDDE":
            lr_res_y, lr_res_x = self.edde_res_y, self.edde_res_x
        elif intended_lr == "ICON":
            lr_res_y, lr_res_x = self.icon_res_y, self.icon_res_x
        else:
            raise ValueError(f"Unsupported intended_LR_data: {intended_lr}")

        self.urma_lr_intended = self.urma.isel(
            y=slice(0, None, lr_res_y),
            x=slice(0, None, lr_res_x),
        )

        self.era5_to_urma_lr_intended = xe.Regridder(
            _maybe_add_bounds(self.era5, self.method),
            _maybe_add_bounds(self.urma_lr_intended, self.method),
            method=self.method,
            reuse_weights=False,
        )
        self.hrrr_to_urma_lr_intended = xe.Regridder(
            _maybe_add_bounds(self.hrrr, self.method),
            _maybe_add_bounds(self.urma_lr_intended, self.method),
            method=self.method,
            reuse_weights=False,
        )
        self.edde_to_urma_lr_intended = xe.Regridder(
            _maybe_add_bounds(self.edde, self.method),
            _maybe_add_bounds(self.urma_lr_intended, self.method),
            method=self.method,
            reuse_weights=False,
        )

        if self.icon_use_mesh:
            need_bounds = self.method in conservative_methods
            target_ds = _maybe_add_bounds(self.urma_lr_intended, self.method)
            grid_out, _, _ = ds_to_ESMFgrid(target_ds, need_bounds=need_bounds, periodic=False)
            icon_global_to_urma_lr = BaseRegridder(
                self.icon_mesh,
                grid_out,
                method=self.method,
                input_dims=(self.icon_input_dim,),
                output_dims=("y", "x"),
                unmapped_to_nan=True,
            )
            grid_out.destroy()
            self.icon_global_to_urma_lr_intended = _wrap_icon_regridder(
                icon_global_to_urma_lr, target_ds
            )
        else:
            if self.method != "nearest_s2d":
                raise ValueError(
                    "ICON locstream regridding only supports nearest_s2d. Enable icon_use_mesh."
                )
            icon_global_to_urma_lr = xe.Regridder(
                self.icon_global,
                self.urma_lr_intended,
                method="nearest_s2d",
                locstream_in=True,
                reuse_weights=False,
            )
            self.icon_global_to_urma_lr_intended = _wrap_icon_regridder(
                icon_global_to_urma_lr, self.urma_lr_intended
            )

# %%
if __name__ == "__main__":
    # %%
    import os
    import dask.distributed as dd

    def auto_configure_dask():
        # Prefer SLURM allocation if present
        cpus = min(4,int(os.environ.get("SLURM_CPUS_PER_TASK", "32")))  
        mem_mb = int(os.environ.get("SLURM_MEM_PER_NODE", "204800"))  # MB
        total_mem_gb = mem_mb // 1024

        n_workers = 4
        threads_per_worker = max(1, cpus // n_workers)
        memory_limit = f"{total_mem_gb // n_workers}GB"

        cluster = dd.LocalCluster(
            n_workers=n_workers,
            threads_per_worker=threads_per_worker,
            memory_limit=memory_limit,
            dashboard_address=":8787",  # fixed port for forwarding
        )
        return cluster

    cluster = auto_configure_dask()
    client = dd.Client(cluster)
    """
    At this stage, if you want to see the dask dashboard onlike, you can use the jupyter server address and prox
    if the jupyter server is http://dgx04.its.albany.edu:8392/lab?token=38dcce3b0ec511590a5325f0ee0f5857
    then, add the proxy and the port to view the dask dashbpard as:
    http://dgx04.its.albany.edu:8392/proxy/8787/status?token=38dcce3b0ec511590a5325f0ee0f5857
    """
    client
    # %%
    import yaml
    cfg_path = Path(__file__).resolve().with_name("baseline_regrid.yaml")
    with open(cfg_path) as f:
        config = yaml.safe_load(f)
    regridders = RegridderRegistry(config)
    # %%
    target = xr.open_zarr(config['paths']['urma_zarr'],chunks=None)
    src = xr.open_zarr(config['paths']['icon_global_zarr'],chunks=None)

    # %%
    input_path = config["paths"]["urma_zarr"]
    target_path = config["paths"]["icon_global_zarr"]
    vars = ['si10','t2m','d2m','sp','tp','u10','v10']

    # %%
    t0 = time.perf_counter()
    ds_full = xr.open_zarr(target_path,consolidated=True)
    regridded = regridders.icon_global_to_urma_hr(ds_full.sel(time=slice('2019-01-01T00', '2019-01-01T05'))).compute()
    dt = time.perf_counter() - t0
    print(f"Regrid elapsed: {dt:.2f}s")

    # %%
    t0 = time.perf_counter()
    ds_full = xr.open_zarr(input_path)
    regridded = xr_uniform_filter(ds_full[vars[0]].sel(time='2019'), size=(1, 5, 5)).compute()
    dt = time.perf_counter() - t0
    print(f"Regrid elapsed: {dt:.2f}s")
# %%
