# met-data-products

Scripts to download, crop, and process meteorological reanalysis and analysis datasets into Zarr stores for the DFS (Deep learning-based Forest fire and Downscaling System) project and related work.

---

## Repository design

| Location | What lives here |
|----------|----------------|
| **`met-data-products/`** (this repo) | Download scripts, processing scripts, Slurm job scripts, full-grid metadata files (`*_full_*`, grid definitions, constant fields) |
| **`DFS/data_prep/`** | URMA regridding scripts (`regrid_to_urma_zarr.py` + slurm wrappers) — DFS-specific, not here |
| **`DFS/data_utils/`** | `RegridderRegistry` and `baseline_regrid.yaml` — single source of truth for regridding logic |
| **`/network/rit/lab/basulab/Projects/DFS/DATA/`** | NYS-domain outputs — cropped orography/LSM, xESMF weight files, Zarr stores, regridded Zarr stores |
| **`/network/rit/lab/basulab/RAW_DATA/`** | Raw downloaded files (GRIB, NetCDF) — only for products that require a raw download step |

NYS-specific artifacts (masks, cropped orography, regrid weights) are intentionally kept in `DFS/DATA` alongside the Zarr stores they belong to, not in this repo.

This repo's responsibility ends at the **native NYS zarr** (e.g. `HRRR_NYS.zarr`, `ERA5_analysis_NYS.zarr`). Regridding to URMA and all downstream DL training data preparation lives in `DFS/data_prep/`.

---

## Dataset inventory

| Dataset | Resolution | Grid type | Temporal coverage | Spatial coverage | Notes |
|---------|-----------|-----------|-------------------|-----------------|-------|
| URMA | 2.5 km | lat-lon | 2014–present | CONUS | Target/reference grid for all regridding |
| RTMA | 2.5 km | lat-lon | 2018–present | CONUS | Precipitation is on a **different grid** from other RTMA variables |
| CONUS404 | 4 km | lat-lon | 1979–2025 | CONUS | From Microsoft Planetary Computer |
| ERA5 | 0.25° | lat-lon | 1940–present | Global | Source: Google ARCO (`gcp-public-data-arco-era5`) |
| EDDEv2 Historical | 12.5 km | lat-lon | 1985–2014 | CONUS | From EPA AWS (`s3://epa-edde-v2`) |
| EDDEv2 SSP2-4.5 | 12.5 km | lat-lon | 2025–2100 | CONUS | From EPA AWS |
| EDDEv2 SSP3-7.0 | 12.5 km | lat-lon | 2025–2100 | CONUS | From EPA AWS |
| ICON-DREAM-Global | ~13 km | **unstructured** | 2010–present | Global | From DWD OpenData; no raw download — processed on the fly |
| HRRR | ~3 km | projected | 2016–present | CONUS | From Utah HRRR Zarr S3 (`s3://hrrrzarr`); no raw download |
| Geomorpho90m | 90 m → 2.5 km | lat-lon | Static | Global | Regridded to URMA 2.5 km NYS grid |

---

## Unit conventions

All datasets are standardised to these units on write:

| Variable | Standard unit | Common source unit | Conversion |
|----------|-------------|-------------------|------------|
| Precipitation (`tp`) | `kg m-2` (≡ mm) | `m` | × 1000 |
| Pressure (`sp`) | `Pa` | `hPa` | × 100 |
| Wind speed / components | `m s-1` | — | — |
| Temperature / dew point | `K` | — | — |
| Wind direction (`wdir10`) | `degrees` | derived | `(270 - atan2(v, u)) % 360` |
| Wind gust (`i10fg`) | `m s-1` | — | — |

---

## Target variable names

Consistent short names used across all zarr stores:

| Short name | Description |
|-----------|-------------|
| `t2m` | 2 m air temperature |
| `d2m` | 2 m dew point temperature |
| `u10` | 10 m eastward wind |
| `v10` | 10 m northward wind |
| `si10` | 10 m wind speed |
| `wdir10` | 10 m wind direction |
| `i10fg` | 10 m instantaneous wind gust |
| `sp` | Surface pressure |
| `tp` | Total precipitation |
| `fsr` | Forecast surface roughness / surface roughness length |
| `sh2` | 2 m specific humidity (HRRR only) |

---

## Per-product notes

### URMA / RTMA

- URMA is the **reference grid** for all NYS regridding: 256 × 288 at 2.5 km (`urma_nys_orography.nc` in `DFS/DATA/URMA_NYS/`).
- RTMA precipitation is on a **different grid** than all other RTMA variables and the URMA grid. Crop scripts handle this separately.
- Precipitation in both products is **instantaneous** (not accumulated), confirmed by visual inspection.
- Snowfall in URMA is added to total precipitation (per NOAA SCN20-45).

### CONUS404

- Fetched from Microsoft Planetary Computer.
- Orography is extracted from geopotential height `Z` at the lowest model level (`bottom_top_stag=0`), converted from m²/s² to m using g = 9.80665.
- Longitude converted from [-180, 180] to [0, 360] for consistency.
- Full orography: `CONUS404/conus404_full_orography.nc`

### ERA5

- **Source:** Google ARCO ERA5 (`gs://gcp-public-data-arco-era5`) — preferred over DestinE because it includes wind gust and has only a ~3-month lag.
- Surface and model-level data are available. Variable registry saved to `ERA5/arco_variable_registry.csv`.
- Precipitation is **not accumulated** over the hours (confirmed by inspection).
- Full static files: `ERA5/era5_full_orography.nc`, `ERA5/era5_full_lsm.nc`, `ERA5/era5_full_z.nc`
- NYS outputs → `DFS/DATA/ERA5_NYS/`

### EDDEv2

- **Source:** EPA AWS (`s3://epa-edde-v2/EDDE_V2/`)
- Three run scenarios in separate Zarr stores: `Historical.zarr` (1985–2014), `SSP2-4.5.zarr` (2025–2100), `SSP3-7.0.zarr` (2025–2100).
- Known issue: variable `hus` is labelled as specific humidity in the documentation but is actually **mixing ratio** (direct WRF output).
- Download uses `s5cmd` for parallel S3 transfer; raw files land in `RAW_DATA/EDDE_V2/hourly/WRF-MPI/`.
- Full orography/LSM: `EDDEv2/eddev2_full_orography.nc`, `EDDEv2/eddev2_full_lsm.nc`
- NYS outputs → `DFS/DATA/EDDEv2_NYS/`

### ICON-DREAM-Global

- **Source:** DWD OpenData (`https://opendata.dwd.de/climate_environment/REA/ICON-DREAM-Global/hourly/`)
- Grid is **unstructured** (triangular mesh, ~2.9 M global nodes). Raw GRIB files are downloaded to `RAW_DATA/ICON-DREAM-Global/` per variable per month.
- Time dimension in source GRIB has forecast steps; processing stacks `(time, step)` → `valid_time` and selects the target month.
- NYS subsetting uses a pre-computed boolean mask (`DFS/DATA/ICON_DREAM_Global_NYS/icon_global_nys_mask.nc`) that selects 7 084 nodes from the global mesh. Generated from `ICON-DREAM-Global/ICON-DREAM-Global_grid.nc` (grid coordinates in radians).
- The NYS zarr retains the unstructured dimension (`values: 7 084`). Regridding to URMA happens as a separate step via `xESMF nearest_s2d`.
- Full grid/constant files: `ICON-DREAM-Global/ICON-DREAM-Global_grid.nc`, `ICON-DREAM-Global/ICON-DREAM-Global_constant_fields.grb`
- NYS outputs → `DFS/DATA/ICON_DREAM_Global_NYS/`

### HRRR

- **Source:** Utah HRRR Zarr public S3 bucket (`s3://hrrrzarr/`), available from 2016-08-23 to present.
- **No raw download** — data is fetched directly per-timestep and written to the local zarr. The S3 path structure is:
  ```
  hrrrzarr/<family>/<YYYYMMDD>/<YYYYMMDD>_<HH>z_<anl|fcst>.zarr/<level>/<var>/<level>/<var>
  ```
- NYS crop is computed from the HRRR chunk index (`s3://hrrrzarr/grid/HRRR_chunk_index.zarr`) using bbox lat 38–48°N, lon 82–68°W.
- Derived variables (`si10`, `wdir10`) are computed from `u10`/`v10` after all source variables are written.
- Zarr is pre-allocated for the full time range (2010–2040); unfilled timesteps are NaN.
- Full orography: `HRRR/hrrr_orography_cropped_nys.nc` is NYS-cropped and lives in `DFS/DATA/HRRR_NYS/`.

### Geomorpho90m

- 90 m static terrain attributes (slope, aspect, TPI, TRI, roughness, etc.) regridded to URMA 2.5 km NYS grid.
- Individual variable files: `Geomorpho90m/2p5km_urma_nys_files/`
- Combined multi-variable file: `DFS/DATA/Geomorpho90m_NYS/geomorpho90m_all_vars_2p5km_urma_nys.nc`

---

## Processing pipeline (per product)

```
Step 1 — Download (products with raw files)           [this repo]
  run_all_*downloads.sh  →  RAW_DATA/<product>/

Step 2 — Process & write to zarr (crop to NYS)        [this repo]
  run_all_process_and_write_to_zarr.sh  →  DFS/DATA/<product>_NYS/<product>_NYS.zarr

Step 3 — Regrid to URMA HR/LR grid                    [DFS/data_prep/]
  DFS/data_prep/<product>/run_all_regrid_to_urma_zarr.sh
    →  DFS/DATA/<product>_NYS/<product>_NYS_to_URMA_HR_bilinear.zarr
```

This repo's pipeline ends at Step 2. Step 3 lives in `DFS/data_prep/` because the URMA target grid, resolution factors, and `RegridderRegistry` are DFS-specific. HRRR skips Step 1 (direct S3 read).

---

## Slurm environment

- **Cluster:** RIT DGX cluster
- **QOS for processing jobs:** `freetier` (8 jobs/user, 256 CPU total)
- **QOS for GPU training jobs:** `16gpu` (4 jobs/user, 16 GPU total)
- **Conda environment:** `/network/rit/lab/basulab/conda_envs/hb533188/DFSAI`
- **Jupyter sessions:** managed via `jx` / `jx-status` (see `~/hpc-jupyter/`)

---

## NYS bounding box

All products are cropped to:

| | Value |
|-|-------|
| Lat min | 38.0° N |
| Lat max | 48.0° N |
| Lon min | 278.0° (= −82° E) |
| Lon max | 292.0° (= −68° E) |

URMA output grid: **256 × 288** (HR), with LR variants at 256/12 × 288/9 (ERA5) and 256/5 × 288/5 (EDDEv2/ICON/HRRR).
