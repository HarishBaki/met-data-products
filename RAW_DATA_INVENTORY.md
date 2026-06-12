# Raw vs. Cropped Data Inventory

Snapshot taken 2026-06-11, ahead of a planned refactor to download raw data in
yearly chunks, crop to NYS, write to zarr, and delete the raw chunk
immediately (instead of downloading+keeping the full raw archive).
`/network/rit/lab/basulab` is at 92% (11 TB free of 121 TB).

Per-product raw vs. cropped storage, variables, and year coverage are below.
"Years saved" reflects *actual non-NaN data* in the cropped zarr stores
(many stores pre-allocate a wider nominal time axis than what's populated).

## Summary table

| Product | Raw storage | Variables downloaded | Years downloaded | Cropped storage | Variables saved | Years saved |
|---|---|---|---|---|---|---|
| URMA | **7.8 TB** | 10 of 14 GRIB fields used (`t2m,d2m,sh2,sp,u10,v10,si10,i10fg,wdir10,tp`) | 2014-01-28 → 2025-12-31 (2014 partial) | **180 GB** main + 153 GB redundant per-year stores | same 10 vars | 2014-2025 (2014 ~91% complete; 2025 ~98.5%) |
| EDDEv2 | **9.6 TB** (+12 GB pilot set; +2.5 TB redundant copies elsewhere) | 11 raw vars downloaded (`hur,pr,ps,psl,td,ts,ua,va,wdirs,wspds,zg500`); only 8 used | Historical 1985-2014 (complete); SSP2-4.5 2025-2074 (gappy); SSP3-7.0 2025-2099 | **96 GB** main (3 stores) + **358 GB** regridded-to-URMA | `t2m,d2m,sp,tp,si10,u10,v10,wdir10` (8 vars) | Historical: 1985-2014 (full); SSP2-4.5 & SSP3-7.0: **only 2025-2030** |
| ICON-DREAM-Global | **21 TB** (16 vars; 7 unused 3-D vars = ~16.3 TB) | 16 raw vars downloaded; 9 used | 2010-01 → 2025-12 (~99% complete) | **21 GB** main + **633 GB** regridded (3 URMA variants + 1 EDDE variant) ≈ **666 GB** | `t2m,d2m,sp,tp,si10,u10,v10,i10fg,fsr,wdir10` (10 vars, 1 derived) | 2010-2025 (2025 partial, through ~mid-Nov) |
| MRMS | **12.1 TB** (8.6 TB + 3.5 TB; `cropped_NYS` intermediate ~3.7 TB est. of that) | 2 raw vars (`MergedReflectivityAtLowestAltitude_00.50`, `MergedReflectivityQCComposite_00.50`) | 2020-10-14 → 2025-08-26 (dense daily) | **105 GB** | `Reflectivity_composite`, `Reflectivity_lowest` | 2020-10-14 → 2025-08-26 (within a 2018-2027 pre-allocated axis) |
| Ouranos | **~52 KB** (by design — raw deleted after each write) | 7 download vars (`tas,ps,huss,pr,hurs,uas,vas`) + 2 derived (`si10,wdir10`) = 9 canonical | 4 of 27 catalog rows pilot-tested so far (see Ouranos section) | **32 GB** (current) → **~1.4 TB target** for planned 8-product × 2018-2100 scope | `t2m,sp,sh2,tp,rh2,u10,v10,si10,wdir10` (9 vars) | varies by row (see Ouranos section); target 2018-2100 |

**Totals**: raw ≈ **53.0 TB** (incl. EDDEv2's redundant 2.5 TB copies) vs. cropped ≈ **1.56 TB** — roughly a **34:1** inflation, i.e. ~51 TB of raw data could eventually be freed once the streaming yearly-chunk pipeline replaces the keep-everything approach. (Ouranos excluded from these totals — see below; it's the *target* pattern, not a cleanup item.)

---

## URMA

- **Raw**: `/network/rit/lab/basulab/RAW_DATA/URMA/` — **7.8 TB**, 4356 daily dirs (`YYYYMMDD`), 2014-01-28 → 2025-12-31, no gaps. Each day = 24× `urma2p5.tHHz.2dvaranl_ndfd.grb2_wexp` (~83 MB, dominates size) + 24× `pcp_01h` + 4× `pcp_06h` grib2 files.
  - The `2dvaranl` GRIB2 contains 14 fields (`tcc, ceil, u10, v10, si10, i10fg, wdir10, t2m, d2m, sh2, sp, vis, orog, swh`); `pcp_01h` contains `tp`.
  - **10 of 14 fields are actually used**: `t2m, d2m, sh2, sp, u10, v10, si10, i10fg, wdir10, tp`. Unused: `tcc, ceil, vis, orog, swh` (a separate static orography file is used instead).
- **Cropped**: `/network/rit/lab/basulab/Projects/DFS/DATA/URMA_NYS/`
  - `URMA_NYS.zarr` (main, 256×288 NYS grid): **180 GB**, same 10 vars, nominal axis 2010-2040 but only **2014-2025 populated** (2014 ~91%, 2015-2025 ~99-100%, 2025 ~98.5%).
  - `2014.zarr` … `2025.zarr` (per-year stores): **153 GB total** — same 10 vars/grid, appear to be an **older per-year layout superseded by `URMA_NYS.zarr`** (2024.zarr is a partial 4.6 GB, 2025.zarr is ~1 MB, suggesting migration was abandoned mid-way). Likely fully redundant — recommend a quick equivalence spot-check, then delete.
  - Aux files (`mask_2d.nc`, `urma_nys_orography.nc`, `urma_percentiles_2018_2023.nc`, `urma_stats_2018_2023.nc`, `nan_times/`): ~570 MB, keep.

## EDDEv2

- **Raw**: `/network/rit/lab/basulab/RAW_DATA/EDDE_V2/hourly/WRF-MPI/` — **9.6 TB** total (Historical 2.3 TB, SSP2-4.5 2.9 TB, SSP3-7.0 4.5 TB). Filenames: `<var>.<scenario>.mpi.EDDE-WRF.<freq>.NA12.<YYYY-MM>.raw.nc`.
  - Variables present: `hur, pr, ps, psl, td, ts, ua, va, wdirs, wspds, zg500` (11). `ua`/`va` only exist 2025-2030 for the SSPs (full 1985-2014 for Historical).
  - Years: Historical 1985-2014 (complete, 30 yrs). SSP2-4.5: 2025-2074 present but uneven (2031-2070 missing `ua`/`va`; 2071-2074 only `ps`/`td`); directory labeled 2025-2100 but **2075-2100 not downloaded**. SSP3-7.0: 2025-2099 present (2031-2099 missing `ua`/`va`); **2100 not downloaded**.
  - `/network/rit/lab/basulab/RAW_DATA/EDDEv2_first_month_files/` — **12 GB**, a separate Jan-2025/SSP2-4.5-only pilot with a different 17-var CORDEX set (`clt,hfls,hfss,hur,hus,pr,ps,rlds,rlut,rsds,td,ts,ua,ustar,va,wdirs,wspds`).
  - **Separate, likely-redundant copies**: `/network/rit/lab/basulab/Projects/DFS/DATA/EDDEv2/EDDEv2_PRESSURE/` (1.2 TB, `ps`+`psl` only) and `EDDEv2/EDDEv2_WIND/` (1.3 TB, `wspds` only) — **2.5 TB combined**, overlapping variables/years already in `RAW_DATA/EDDE_V2`. Owned partly by other lab users (`kn329846`, `sb454517`) — coordinate before deleting.
- **Cropped**: `/network/rit/lab/basulab/Projects/DFS/DATA/EDDEv2_NYS/hourly/WRF-MPI/`
  - `Historical.zarr` (68 GB), `SSP2-4.5.zarr` (14 GB), `SSP3-7.0.zarr` (14 GB) — 8 vars each: `t2m, d2m, sp, tp, si10, u10, v10, wdir10` (112×114 NYS grid).
  - Years populated: **Historical fully 1985-2014**; **SSP2-4.5 and SSP3-7.0 only 2025-2030** (6 of 76 pre-allocated years) — the cropping pipeline hasn't been run past 2030 even though raw SSP3-7.0 data exists through 2099.
  - Regridded-to-URMA derivatives: `{SSP2-4.5,SSP3-7.0}_to_URMA_HR_{bilinear,conservative,nearest_s2d}.zarr` — **358 GB total** (75+75+73+73+31+31 GB), same 2025-2030 coverage.
  - Other: `nan_times_*` (~114 MB), `NY_EDDEv2_*` downstream analysis outputs (752 MB), orography/lsm + xesmf weight files (~11 MB).
  - **Caveat for the refactor**: if raw SSP data is deleted now, re-running the crop for 2031+ for SSP2-4.5 (2031-2074, with `ua`/`va` gaps after 2030) and SSP3-7.0 (2031-2099) would require **re-downloading from `s3://epa-edde-v2/EDDE_V2/hourly`**.

## ICON-DREAM-Global

- **Raw**: `/network/rit/lab/basulab/RAW_DATA/ICON-DREAM-Global/` — **21 TB**, 16 variable dirs, monthly GRIB files (`ICON-DREAM-Global_YYYYMM_<VAR>_hourly.grb`), 2010-01 → 2025-12 (T_2M complete 192/192 months; DEN missing 3 months — overall ~99% complete).
  - **9 of 16 vars feed the cropped product**: `PS→sp, T_2M→t2m, TD_2M→d2m, TOT_PREC→tp, U_10M→u10, V_10M→v10, VMAX_10M→i10fg, WS_10M→si10, Z0→fsr` (sizes 94G-536G each).
  - **7 of 16 vars are unused 3-D/model-level fields**: `DEN(2.5T), P(94G), QV(696G), TKE(3.3T), U(3.5T), V(3.5T), WS(3.5T)` — **~16.3 TB (78% of raw)**, not referenced anywhere in `process_and_write_to_zarr.py`. **Largest single deletion candidate in this entire inventory.**
- **Cropped**: `/network/rit/lab/basulab/Projects/DFS/DATA/ICON_DREAM_Global_NYS/`
  - `ICON_DREAM_Global_NYS.zarr` (main, unstructured `values=7084` NYS-masked points): **21 GB**, 10 vars (`d2m, fsr, i10fg, si10, sp, t2m, tp, u10, v10, wdir10` — `wdir10` derived from `u10`/`v10`).
  - Years: nominal 2010-2025 hourly (140,256 steps), **essentially complete 2010-2024** (only single-digit-hour gaps in 2010/2015/2017/2020), **2025 partial** (8020/8760 hrs, through ~mid-Nov).
  - Regridded derivatives: `_to_URMA_HR_bilinear` (253 GB), `_to_URMA_HR_conservative` (249 GB), `_to_URMA_HR_nearest_s2d` (131 GB), `_to_EDDE_LR_bilinear` (12 GB) — **~645 GB total**.
  - Aux (mask/orography/weights/nan_times): ~48 MB.

## MRMS

*(Different project layout — raw lives under `Harish/Gust_field_nowcasting_from_Sparse_stations/data/`, cropped under `Projects/WISER/DATA/`.)*

- **Raw**: `/network/rit/lab/basulab/Harish/Gust_field_nowcasting_from_Sparse_stations/data/MRMS_grib_data/CONUS/` — two variable dirs, each with 1778 daily subdirs (`YYYYMMDD`, 2020-10-14 → 2025-08-26, dense/complete daily coverage at ~720 files/day = 2-min cadence; 93 dates flagged with missing cropped output per `missing_dates.txt`).
  - `MergedReflectivityAtLowestAltitude_00.50/` → `Reflectivity_lowest`: **8.6 TB** (confirmed via `du -sh`; `unzipped` raw grib2 ~6.8 TB + `cropped_NYS` intermediate ~1.8 TB est.)
  - `MergedReflectivityQCComposite_00.50/` → `Reflectivity_composite`: **3.5 TB** (confirmed via `du -sh`; `unzipped` ~1.7 TB + `cropped_NYS` ~1.8 TB est.)
  - **Combined ~3.7 TB (est.) is the `cropped_NYS/*.nc` intermediate** (per-file `cdo remapbil` output already regridded to the 256×288 NYS orography grid) — a separate staging product consumed by `Process_and_write_to_zarr_MRMS_daily.py`, redundant once `MRMS.zarr` is verified complete.
- **Cropped**: `/network/rit/lab/basulab/Projects/WISER/DATA/MRMS.zarr` — **105 GB**, 2 vars (`Reflectivity_composite`, `Reflectivity_lowest`, dBZ, 256×288 NYS grid).
  - Nominal axis 2018-01-01 → 2027-12-31 (5-min, 1,051,776 steps), but **actual data only 2020-10-14 → 2025-08-26** (2018-2020-10-13 and 2025-08-27→2027 are all-NaN placeholders, ~53% of the axis).

## Ouranos — reference architecture for the refactor

Ouranos (CRCM5-CMIP6 NAM-12, 27-row catalog of ERA5-evaluation + 4 GCMs ×
historical/SSP scenarios/realizations) **already implements the
download-yearly-chunk → crop/derive → write-zarr → delete-raw pattern** that
the refactor aims to bring to the other 4 products. It's included here both
for completeness and as the architectural template, since it will be central
to upcoming deliverables.

- **Raw**: `/network/rit/lab/basulab/RAW_DATA/Ouranos/1hr/` — **52 KB**, empty
  directory skeletons only. `process_and_write_to_zarr.py` fetches one
  (catalog-row, variable, year) NCSS chunk at a time (already NY-cropped
  server-side), writes it directly into the canonical zarr store, then
  deletes the raw NetCDF — raw never accumulates, by design.
- **Variables**: 7 "download" vars via NCSS, renamed CORDEX→canonical
  (`tas→t2m, ps→sp, huss→sh2, pr→tp, hurs→rh2, uas→u10, vas→v10`), plus 2
  "derived" vars computed in-place from the just-written `u10`/`v10`
  (`si10 = sqrt(u10²+v10²)`, `wdir10 = (270-atan2(v10,u10))%360`) — no extra
  download. **9 canonical vars total**, same names/units as the other
  products' cropped stores (`data_utils/var_meta.yaml`).
- **Cropped** (current): `/network/rit/lab/basulab/Projects/DFS/DATA/Ouranos_NYS/` — **32 GB**, across 4 of 27 catalog rows pilot/validation-tested so far:

  | Catalog row | Size | Years with data |
  |---|---|---|
  | ERA5-evaluation v1-r1 (idx 0) | 9.9 GB | 1979, 2017-2020 |
  | ERA5-evaluation v1-r2 (idx 1) | 388 KB | (init only, no years written) |
  | CNRM-ESM2-1 ssp370 (idx 5) | 18 GB | 2018-2025 (8 yrs) |
  | CanESM5 historical (idx 6) | 4.3 GB | 1950, 1952 |

- **Planned full scope**: upcoming deliverables call for **8 products (4 GCMs
  × 2 SSPs) × 2018-2100** (83 yrs) = 664 store-years. At the
  empirically-observed **~2.13 GB/store-year** (averaged across the 3
  populated stores above), that's **~1.4 TB** — ~13% of the 11 TB currently
  free, comfortably within headroom even before any of the deletion
  candidates below are acted on. (Adding 1950-2014 historical for all 4 GCMs
  would add ~554 GB more → ~2 TB grand total.)

---

## Deletion-candidate ranking (largest / safest first)

1. **ICON-DREAM-Global unused 3-D fields** (`DEN, P, QV, TKE, U, V, WS`) — **~16.3 TB**. Not used anywhere downstream; safe to delete outright (no re-crop dependency).
2. **MRMS `unzipped/` raw grib2 + `cropped_NYS/` intermediates** — **12.1 TB** (8.6+3.5 TB confirmed). `MRMS.zarr` already covers the full 2020-2025 raw range; verify completeness then delete both staging layers.
3. **EDDEv2 main raw `hourly/WRF-MPI/`** — **9.6 TB**. Needed to re-crop SSP2-4.5 (2031-2074) and SSP3-7.0 (2031-2099), which haven't been processed yet — **don't delete until the refactored per-year pipeline has consumed these years**, or accept re-downloading from S3 later.
4. **URMA raw GRIB2** — **7.8 TB**. Cropped product (`URMA_NYS.zarr`) already covers the full 2014-2025 range — safe to delete once confirmed (consider re-download cost from NOAA archive if ever needed again).
5. **EDDEv2 `EDDEv2_PRESSURE` + `EDDEv2_WIND`** — **2.5 TB**. Overlaps variables already in raw EDDE_V2; coordinate with other owning users before deleting.
6. **ICON-DREAM-Global remaining 9 raw vars** (PS, T_2M, TD_2M, TOT_PREC, U_10M, V_10M, VMAX_10M, WS_10M, Z0 — ~4.6 TB) and **URMA per-year `*.zarr`** (153 GB) — only delete after the refactor's resumability is confirmed (these are the ones a re-run would otherwise need).
7. **EDDEv2 first-month pilot set** (12 GB) — trivial, low priority.
