#!/usr/bin/env python3
# %%
"""
scripts/data_processing/process_mrms.py

Streaming MRMS pipeline: S3 → in-memory decompress → bilinear regrid → Zarr.
No intermediate files are written to disk.  One Zarr store holds all variables.

Subcommands
-----------
process   Download, regrid, and write MRMS data for a date range.
extend    Extend the Zarr time axis to a new end date.
check     Report which days are missing for given variables.

Usage examples
--------------
# Process two variables
python process_mrms.py --config configs/mrms.yaml process \\
    --variables MergedReflectivityQCComposite_00.50 \\
                MergedReflectivityAtLowestAltitude_00.50 \\
    --process-start 20201014 --process-end 20250826

# Add a new variable to an existing store
python process_mrms.py --config configs/mrms.yaml process \\
    --variables EchoTop_18_00.50 \\
    --process-start 20201014 --process-end 20250826

# Extend the time axis to 2029
python process_mrms.py --config configs/mrms.yaml extend --full-end 20291231

# Dry-run extension (shows planned changes, writes nothing)
python process_mrms.py --config configs/mrms.yaml extend --full-end 20291231 --dry-run

# Check missing days
python process_mrms.py --config configs/mrms.yaml check \\
    --variables MergedReflectivityQCComposite_00.50 \\
    --process-start 20201014 --process-end 20250826
"""
from __future__ import annotations

import argparse
import gzip
import json
import os
import re
import shutil
import subprocess
import sys
import tempfile
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

import dask.array as da
import numpy as np
import pandas as pd
import xarray as xr
import yaml
import zarr
from scipy.interpolate import RegularGridInterpolator
from tqdm import tqdm

# ─────────────────────────────────────────────────────────────
# Project paths
# ─────────────────────────────────────────────────────────────
PROJECT_ROOT = Path(__file__).resolve().parents[2]
SRC_DIR = PROJECT_ROOT / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from project_paths import DATA_DIR, STATIC_DATA_DIR  # noqa: E402

DEFAULT_CONFIG = PROJECT_ROOT / "configs" / "mrms.yaml"


# ─────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────

def is_interactive() -> bool:
    import __main__ as main
    return not hasattr(main, "__file__") or "ipykernel" in sys.argv[0]


def load_config(path: str | Path) -> dict:
    with open(path) as f:
        return yaml.safe_load(f)


def resolve_path(p: str) -> Path:
    """Resolve relative paths against PROJECT_ROOT; leave absolute paths unchanged."""
    p = Path(p)
    return p if p.is_absolute() else PROJECT_ROOT / p


def build_full_dates(full_start: str, full_end: str, freq: str) -> pd.DatetimeIndex:
    start = pd.to_datetime(full_start, format="%Y%m%d")
    end   = pd.to_datetime(full_end,   format="%Y%m%d")
    # Include every timestep up to 23:55 on the last day
    end_inclusive = end + pd.Timedelta(hours=23, minutes=55)
    return pd.date_range(start=start, end=end_inclusive, freq=freq)


def _get_grid_lat_lon(grid_ds: xr.Dataset) -> tuple[xr.DataArray, xr.DataArray]:
    """Return 2-D latitude/longitude arrays from a grid dataset with flexible naming."""
    lat = None
    lon = None

    for name in ("latitude", "lat"):
        if name in grid_ds:
            lat = grid_ds[name]
            break

    for name in ("longitude", "lon"):
        if name in grid_ds:
            lon = grid_ds[name]
            break

    if lat is None:
        for var_name, da_var in grid_ds.variables.items():
            if da_var.attrs.get("standard_name") == "latitude":
                lat = grid_ds[var_name]
                break

    if lon is None:
        for var_name, da_var in grid_ds.variables.items():
            if da_var.attrs.get("standard_name") == "longitude":
                lon = grid_ds[var_name]
                break

    if lat is None or lon is None:
        raise ValueError(
            f"{grid_ds.encoding.get('source', 'grid dataset')}: could not find latitude/longitude "
            f"variables. Available variables: {list(grid_ds.variables)}"
        )

    if lat.ndim != 2 or lon.ndim != 2:
        raise ValueError("Grid latitude/longitude must both be 2-D arrays.")

    return lat, lon


# ─────────────────────────────────────────────────────────────
# Zarr store management
# ─────────────────────────────────────────────────────────────

def _empty_dataarray(
    full_dates: pd.DatetimeIndex,
    var_name: str,
    var_cfg: dict,
    orog_path: Path,
    zarr_cfg: dict,
) -> xr.DataArray:
    grid_ds = xr.open_dataset(orog_path)
    grid_ds.attrs = {}
    lat2d, lon2d = _get_grid_lat_lon(grid_ds)
    tc, yc, xc = zarr_cfg["time_chunk"], zarr_cfg["y_chunk"], zarr_cfg["x_chunk"]
    shape = (len(full_dates),) + tuple(lat2d.shape)
    data = da.full(shape, np.nan, chunks=(tc, yc, xc), dtype="float32")
    return xr.DataArray(
        data,
        dims=("time", "y", "x"),
        coords={
            "time":      full_dates,
            "latitude":  (("y", "x"), lat2d.values.astype(np.float64)),
            "longitude": (("y", "x"), lon2d.values.astype(np.float64)),
        },
        name=var_name,
        attrs={
            "long_name":     var_cfg["long_name"],
            "units":         var_cfg["units"],
            "_FillValue":    np.nan,
            "missing_value": np.nan,
        },
    )


def _write_variable_to_store(
    zarr_path: Path,
    full_dates: pd.DatetimeIndex,
    var_name: str,
    var_cfg: dict,
    orog_path: Path,
    zarr_cfg: dict,
    mode: str,
    write_global_attrs: bool,
) -> None:
    da_arr = _empty_dataarray(full_dates, var_name, var_cfg, orog_path, zarr_cfg)
    ds = da_arr.to_dataset()
    if write_global_attrs:
        ds.attrs = {
            "title":       "MRMS Reflectivity Dataset",
            "source":      "NOAA MRMS, bilinear-remapped to orography grid",
            "Conventions": "CF-1.8",
            "history":     f"Initialized {pd.Timestamp.now().isoformat()}",
        }
    ds.to_zarr(
        str(zarr_path),
        mode=mode,
        compute=False,
        zarr_format=zarr_cfg.get("zarr_format", 2),
    )


def ensure_initialized(
    zarr_path: Path,
    full_dates: pd.DatetimeIndex,
    var_name: str,
    var_cfg: dict,
    orog_path: Path,
    zarr_cfg: dict,
) -> None:
    """
    Guarantee the Zarr store exists and contains *var_name* on the correct time axis.

    Cases handled:
      1. Store does not exist   → create from scratch with global attrs.
      2. Store exists, variable missing → append variable (no touch to global attrs).
      3. Store exists, variable present → verify time axis consistency.
    """
    if not zarr_path.exists():
        print(f"[init] Creating {zarr_path.name}  variable='{var_name}'")
        _write_variable_to_store(
            zarr_path, full_dates, var_name, var_cfg, orog_path, zarr_cfg,
            mode="w", write_global_attrs=True,
        )
        return

    ds_meta = xr.open_zarr(str(zarr_path), consolidated=False)

    if "time" not in ds_meta.coords:
        raise ValueError(f"{zarr_path}: Zarr store has no 'time' coordinate.")

    # ── Time axis consistency check ──────────────────────────
    same_len = ds_meta.sizes.get("time", -1) == len(full_dates)
    same_vals = same_len and np.array_equal(
        pd.to_datetime(ds_meta.time.values), pd.to_datetime(full_dates)
    )
    if not same_vals:
        raise ValueError(
            f"{zarr_path}: time-axis mismatch with requested full_dates "
            f"({ds_meta.sizes.get('time')} vs {len(full_dates)} steps). "
            "Run the 'extend' subcommand or rebuild the store."
        )

    if var_name not in ds_meta.data_vars:
        print(f"[init] Adding variable '{var_name}' to {zarr_path.name}")
        _write_variable_to_store(
            zarr_path, full_dates, var_name, var_cfg, orog_path, zarr_cfg,
            mode="a", write_global_attrs=False,
        )
    else:
        print(f"[init] '{var_name}' already present in {zarr_path.name} — OK")


# ─────────────────────────────────────────────────────────────
# Time-axis extension
# ─────────────────────────────────────────────────────────────

def extend_time_axis(
    zarr_path: Path,
    new_full_end: str,
    freq: str = "5min",
    dry_run: bool = False,
) -> None:
    """
    Extend every time-first array in the Zarr store to cover up to new_full_end.

    Works with xarray's default zarr v2 time encoding (int64 nanoseconds since
    1970-01-01, or hours/minutes since an origin — both handled).
    """
    root = zarr.open_group(str(zarr_path), mode="a" if not dry_run else "r")

    if "time" not in root.array_keys():
        raise ValueError(f"No 'time' array in {zarr_path}")

    # ── Decode existing time coordinate ──────────────────────
    time_arr   = root["time"]
    raw_values = np.asarray(time_arr[:], dtype=np.int64)

    zattrs_path = zarr_path / "time" / ".zattrs"
    zattrs      = json.loads(zattrs_path.read_text()) if zattrs_path.exists() else {}
    units       = zattrs.get("units", "")

    units_re = re.compile(r"^(?P<unit>\w+)\s+since\s+(?P<origin>.+)$")
    m = units_re.match(units)

    if m:
        unit   = m.group("unit").lower()
        origin = pd.Timestamp(m.group("origin"))
        if unit in ("nanoseconds", "nanosecond"):
            existing_times = pd.to_datetime(raw_values, unit="ns")
        elif unit in ("minutes", "minute"):
            existing_times = pd.to_datetime(
                [origin + pd.Timedelta(minutes=int(v)) for v in raw_values]
            )
        elif unit in ("hours", "hour"):
            existing_times = pd.to_datetime(
                [origin + pd.Timedelta(hours=int(v)) for v in raw_values]
            )
        else:
            raise ValueError(f"Unsupported time unit in zarr store: {units!r}")
    else:
        # Fallback: assume int64 nanoseconds (xarray default for zarr v2)
        existing_times = pd.to_datetime(raw_values, unit="ns")

    existing_end = pd.Timestamp(existing_times[-1])
    target_end   = pd.Timestamp(
        pd.to_datetime(new_full_end, format="%Y%m%d")
        + pd.Timedelta(hours=23, minutes=55)
    )

    if target_end <= existing_end:
        print(f"[extend] Store already covers {existing_end}. Nothing to do.")
        return

    # ── Build new timestamps ──────────────────────────────────
    step       = pd.Timedelta(freq)
    new_times  = pd.date_range(start=existing_end + step, end=target_end, freq=freq)
    old_len    = len(raw_values)
    new_len    = old_len + len(new_times)

    # Encode new timestamps the same way xarray encoded the existing ones
    if m and unit in ("minutes", "minute"):
        new_raw = ((new_times - origin) / pd.Timedelta(minutes=1)).astype(np.int64)
    elif m and unit in ("hours", "hour"):
        new_raw = ((new_times - origin) / pd.Timedelta(hours=1)).astype(np.int64)
    else:
        new_raw = new_times.astype(np.int64)   # nanoseconds since epoch

    # ── Report plan ──────────────────────────────────────────
    arrays_to_resize = []
    for arr_name in sorted(root.array_keys()):
        arr_zattrs_path = zarr_path / arr_name / ".zattrs"
        if arr_zattrs_path.exists():
            dims = json.loads(arr_zattrs_path.read_text()).get("_ARRAY_DIMENSIONS", [])
            if dims and dims[0] == "time" and arr_name != "time":
                arrays_to_resize.append(arr_name)

    print(
        f"[extend] {zarr_path.name}\n"
        f"  existing end : {existing_end}\n"
        f"  target end   : {target_end}\n"
        f"  adding       : {len(new_times)} timesteps\n"
        f"  arrays       : {['time'] + arrays_to_resize}"
    )

    if dry_run:
        print("[extend] dry-run — no changes written.")
        return

    # ── Resize and write ─────────────────────────────────────
    for arr_name in arrays_to_resize:
        arr = root[arr_name]
        new_shape = (new_len,) + arr.shape[1:]
        arr.resize(new_shape)
        print(f"  resized {arr_name}: {arr.shape} → {new_shape}")

    time_arr.resize((new_len,))
    root["time"][old_len:new_len] = new_raw
    print(f"[extend] Done — store now covers to {target_end}")


# ─────────────────────────────────────────────────────────────
# Checking
# ─────────────────────────────────────────────────────────────

def check_day_in_zarr(
    zarr_path: Path,
    day_str: str,
    var_name: str,
    freq: str = "5min",
) -> bool:
    """Return True if *day_str* has at least one non-NaN value for *var_name*."""
    ds = xr.open_zarr(str(zarr_path), consolidated=False)
    if var_name not in ds.data_vars:
        return False
    day_dt    = pd.to_datetime(day_str, format="%Y%m%d")
    day_times = pd.date_range(start=day_dt, periods=288, freq=freq)
    try:
        vals = ds[var_name].sel(
            time=day_times,
            method="nearest",
            tolerance=pd.Timedelta("1min"),
        )
        return bool(vals.notnull().any().compute())
    except KeyError:
        return False


def find_missing_days(
    zarr_path: Path,
    days: list[pd.Timestamp],
    var_name: str,
    freq: str = "5min",
) -> list[str]:
    """Return day strings missing from the store (open zarr once for all days)."""
    if not zarr_path.exists():
        return [d.strftime("%Y%m%d") for d in days]

    ds = xr.open_zarr(str(zarr_path), consolidated=False)
    if var_name not in ds.data_vars:
        return [d.strftime("%Y%m%d") for d in days]

    # Compute a per-timestep has-data flag across the full spatial grid.
    all_times = pd.DatetimeIndex(
        [t for d in days
         for t in pd.date_range(start=d, periods=288, freq=freq)]
    )
    try:
        has_data = (
            ds[var_name]
            .sel(time=all_times, method="nearest", tolerance=pd.Timedelta("1min"))
            .notnull()
            .any(dim=("y", "x"))
            .compute()
        )
    except Exception:
        return [d.strftime("%Y%m%d") for d in days]

    missing = []
    for d in days:
        day_times = pd.date_range(start=d, periods=288, freq=freq)
        try:
            day_flags = has_data.sel(time=day_times)
            if not bool(day_flags.any()):
                missing.append(d.strftime("%Y%m%d"))
        except Exception:
            missing.append(d.strftime("%Y%m%d"))
    return missing


# ─────────────────────────────────────────────────────────────
# Regridding — built once, reused for all files
# ─────────────────────────────────────────────────────────────

def load_target_grid(orog_path: Path) -> tuple[np.ndarray, tuple[int, int]]:
    """
    Load the target (lat, lon) grid from the orography file.

    Returns
    -------
    query_points : (N, 2) float64  — (lat, lon) pairs for every target pixel
    out_shape    : (y, x)          — spatial shape of the target grid
    """
    grid_ds = xr.open_dataset(orog_path)
    lat_da, lon_da = _get_grid_lat_lon(grid_ds)
    lat2d = lat_da.values.astype(np.float64)    # (y, x)
    lon2d = lon_da.values.astype(np.float64)    # (y, x)
    query_points = np.stack([lat2d.ravel(), lon2d.ravel()], axis=1)
    return query_points, lat2d.shape


def regrid_frame(
    data2d:       np.ndarray,
    src_lats:     np.ndarray,
    src_lons:     np.ndarray,
    query_points: np.ndarray,
    out_shape:    tuple[int, int],
) -> np.ndarray:
    """
    Bilinear interpolation of a 2-D MRMS frame onto the target grid.

    *src_lats* must be monotonically increasing.  Callers are responsible for
    flipping north-to-south MRMS arrays before passing here.
    """
    interp = RegularGridInterpolator(
        (src_lats, src_lons),
        data2d,
        method="linear",
        bounds_error=False,
        fill_value=np.nan,
    )
    return interp(query_points).reshape(out_shape).astype(np.float32)


# ─────────────────────────────────────────────────────────────
# S3 download
# ─────────────────────────────────────────────────────────────

def download_day_to_tmpdir(
    bucket:      str,
    prefix:      str,
    product:     str,
    day:         str,
    tmpdir:      str,
    num_workers: int,
) -> int:
    """
    Download all *.grib2.gz files for *product*/*day* to *tmpdir* via s5cmd.
    Returns number of files downloaded.
    """
    s3_glob = f"s3://{bucket}/{prefix}/{product}/{day}/*.grib2.gz"
    result = subprocess.run(
        [
            "s5cmd", "--no-sign-request",
            "--numworkers", str(num_workers),
            "sync", s3_glob, tmpdir + "/",
        ],
        capture_output=True, text=True,
    )
    if result.returncode != 0:
        print(f"[warn] s5cmd failed for {product}/{day}: {result.stderr[:300]}")
    return len(list(Path(tmpdir).glob("*.grib2.gz")))


# ─────────────────────────────────────────────────────────────
# Per-file processing
# ─────────────────────────────────────────────────────────────

_TS_RE = re.compile(r"(\d{8})-(\d{6})")


def _parse_timestamp(path: Path) -> pd.Timestamp | None:
    m = _TS_RE.search(path.name)
    return pd.to_datetime(f"{m.group(1)}{m.group(2)}", format="%Y%m%d%H%M%S") if m else None


def process_single_gz_file(
    gz_path:      Path,
    var_cfg:      dict,
    query_points: np.ndarray,
    out_shape:    tuple[int, int],
) -> tuple[pd.Timestamp, np.ndarray] | None:
    """
    Decompress one .grib2.gz → read with cfgrib → bilinear-regrid to target grid.

    Returns (rounded_5min_timestamp, regridded_float32_array) or None on failure.
    The .grib2 bytes are never written to a persistent location — only a small
    NamedTemporaryFile that is deleted immediately after cfgrib reads it.
    """
    ts_raw = _parse_timestamp(gz_path)
    if ts_raw is None:
        return None

    ts = ts_raw.round("5min")
    if abs((ts - ts_raw).total_seconds()) > 120:   # >2 min from slot → skip
        return None

    tmp_path = None
    try:
        raw_bytes = gzip.decompress(gz_path.read_bytes())

        # cfgrib needs a real file path; use a temp file deleted immediately after read
        fd, tmp_path = tempfile.mkstemp(suffix=".grib2")
        try:
            os.write(fd, raw_bytes)
        finally:
            os.close(fd)

        ds = xr.open_dataset(
            tmp_path,
            engine="cfgrib",
            backend_kwargs={"indexpath": ""},
        )

        varname = list(ds.data_vars)[0]
        data    = ds[varname].values

        # Squeeze any leading size-1 dimensions (e.g. time=1 from some products)
        while data.ndim > 2 and data.shape[0] == 1:
            data = data[0]
        if data.ndim != 2:
            return None

        # Latitude coordinate
        lats = (ds.latitude.values  if "latitude"  in ds.coords else
                ds.lat.values       if "lat"       in ds.coords else None)
        lons = (ds.longitude.values if "longitude" in ds.coords else
                ds.lon.values       if "lon"       in ds.coords else None)
        if lats is None or lons is None:
            return None

        # Ensure lats are monotonically increasing (MRMS is N→S)
        if lats[0] > lats[-1]:
            lats = lats[::-1]
            data = data[::-1, :]

        # Replace sentinel values with NaN
        data = data.astype(np.float32)
        mv  = float(var_cfg.get("missing_value",     -99.0))
        ncv = float(var_cfg.get("no_coverage_value", -999.0))
        data[(data == mv) | (data == ncv) | (data < -90)] = np.nan

        regridded = regrid_frame(data, lats, lons, query_points, out_shape)
        return ts, regridded

    except Exception as exc:
        print(f"[warn] {gz_path.name}: {exc}")
        return None

    finally:
        if tmp_path and os.path.exists(tmp_path):
            os.unlink(tmp_path)


# ─────────────────────────────────────────────────────────────
# Per-day pipeline
# ─────────────────────────────────────────────────────────────

def process_and_write_day(
    day_str:      str,
    product:      str,
    var_name:     str,
    var_cfg:      dict,
    zarr_path:    Path,
    full_dates:   pd.DatetimeIndex,
    s3_cfg:       dict,
    query_points: np.ndarray,
    out_shape:    tuple[int, int],
    zarr_cfg:     dict,
    freq:         str = "5min",
    n_workers:    int = 8,
) -> None:
    """
    Full pipeline for one calendar day:
      1. Skip if data already present in the Zarr store.
      2. Download all *.grib2.gz files to a temp directory (deleted on exit).
      3. Decompress + cfgrib-read + bilinear-regrid each file in a thread pool.
      4. Assemble a 288-slot NaN-filled array; fill slots with successful results.
      5. Write to Zarr via a single region write.
    """
    if check_day_in_zarr(zarr_path, day_str, var_name, freq):
        print(f"[skip] {day_str}  {var_name}  already in store")
        return

    tmpdir = tempfile.mkdtemp(prefix=f"mrms_{day_str}_")
    try:
        # ── Download ─────────────────────────────────────────
        n_dl = download_day_to_tmpdir(
            s3_cfg["bucket"], s3_cfg["prefix"],
            product, day_str, tmpdir,
            s3_cfg.get("num_workers", 32),
        )
        if n_dl == 0:
            print(f"[skip] {day_str}  {product}: no files on S3")
            return
        print(f"[dl]   {day_str}  {product}: {n_dl} files downloaded")

        # ── Process files in parallel threads ────────────────
        gz_files = sorted(Path(tmpdir).glob("*.grib2.gz"))
        results: dict[pd.Timestamp, np.ndarray] = {}

        with ThreadPoolExecutor(max_workers=n_workers) as pool:
            futures = {
                pool.submit(process_single_gz_file, f, var_cfg, query_points, out_shape): f
                for f in gz_files
            }
            for fut in as_completed(futures):
                r = fut.result()
                if r is not None:
                    ts, data = r
                    results[ts] = data

        if not results:
            print(f"[warn] {day_str}  {var_name}: no usable frames after processing")
            return

        # ── Assemble 288-slot day array ───────────────────────
        day_dt    = pd.to_datetime(day_str, format="%Y%m%d")
        day_times = pd.date_range(start=day_dt, periods=288, freq=freq)
        ny, nx    = out_shape
        day_array = np.full((288, ny, nx), np.nan, dtype=np.float32)

        for ts, frame in results.items():
            try:
                slot = day_times.get_loc(ts)
                day_array[slot] = frame
            except KeyError:
                pass  # timestamp fell outside the day window (rare)

        n_written = int(np.sum(np.any(~np.isnan(day_array), axis=(1, 2))))
        print(f"[proc] {day_str}  {var_name}: {n_written}/288 slots populated")

        # ── Region write to Zarr ──────────────────────────────
        global_idx = pd.Index(full_dates).get_indexer(day_times)
        if (global_idx < 0).any():
            print(
                f"[warn] {day_str}: some timestamps lie outside the global calendar "
                "(full_start/full_end). Run 'extend' or widen the range. Skipping."
            )
            return

        ds_chunk = xr.Dataset(
            {
                var_name: xr.DataArray(
                    day_array,
                    dims=("time", "y", "x"),
                    coords={"time": day_times},
                )
            }
        ).chunk({"time": zarr_cfg["time_chunk"]})

        region = {"time": slice(int(global_idx[0]), int(global_idx[-1]) + 1)}
        ds_chunk.to_zarr(
            str(zarr_path),
            region=region,
            mode="a",
            zarr_format=zarr_cfg.get("zarr_format", 2),
        )
        print(f"[write] {day_str}  {var_name} → region {region}")

    finally:
        shutil.rmtree(tmpdir, ignore_errors=True)


# ─────────────────────────────────────────────────────────────
# CLI
# ─────────────────────────────────────────────────────────────

def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "MRMS streaming pipeline — S3 → regrid → Zarr, no intermediate files.\n"
            "Config file controls S3, grid, zarr, and variable metadata.\n"
            "CLI args control what to process and when."
        ),
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    parser.add_argument(
        "--config", "-c",
        default=str(DEFAULT_CONFIG),
        help=f"Path to YAML config (default: {DEFAULT_CONFIG.relative_to(PROJECT_ROOT)})",
    )

    sub = parser.add_subparsers(dest="subcommand", required=not is_interactive())

    # ── process ──────────────────────────────────────────────
    p = sub.add_parser("process", help="Download, regrid, write data for a date range.")
    p.add_argument(
        "--variables", "-v", nargs="+", required=True,
        metavar="PRODUCT",
        help="One or more S3 product folder names (keys in config 'variables' section).",
    )
    p.add_argument(
        "--process-start", required=True, metavar="YYYYMMDD",
        help="First day to process (inclusive).",
    )
    p.add_argument(
        "--process-end", required=True, metavar="YYYYMMDD",
        help="Last day to process (inclusive).",
    )
    p.add_argument(
        "--full-start", default=None, metavar="YYYYMMDD",
        help="Global time-axis start.  Defaults to 'full_start' in the config.",
    )
    p.add_argument(
        "--full-end", default=None, metavar="YYYYMMDD",
        help="Global time-axis end.  Defaults to 'full_end' in the config.",
    )
    p.add_argument(
        "--workers", type=int, default=None,
        help="Parallel threads per day for file processing (default: from config).",
    )
    p.add_argument(
        "--dry-run", action="store_true",
        help="Print what would be done without downloading or writing anything.",
    )

    # ── extend ───────────────────────────────────────────────
    e = sub.add_parser(
        "extend",
        help="Extend the Zarr time axis to a new end date (resize in place).",
    )
    e.add_argument(
        "--full-end", required=True, metavar="YYYYMMDD",
        help="New end date for the global time axis.",
    )
    e.add_argument(
        "--dry-run", action="store_true",
        help="Print planned changes without modifying the store.",
    )

    # ── check ────────────────────────────────────────────────
    c = sub.add_parser("check", help="Report missing days for given variables.")
    c.add_argument(
        "--variables", "-v", nargs="+", required=True, metavar="PRODUCT",
        help="S3 product names to check.",
    )
    c.add_argument(
        "--process-start", required=True, metavar="YYYYMMDD",
        help="Start of the check window.",
    )
    c.add_argument(
        "--process-end", required=True, metavar="YYYYMMDD",
        help="End of the check window.",
    )

    return parser


# ─────────────────────────────────────────────────────────────
# Main
# ─────────────────────────────────────────────────────────────

# %%
if __name__ == "__main__":
    # ── Interactive defaults (Jupyter / #%% runner) ──────────
    if is_interactive():
        sys.argv = [
            sys.argv[0],
            "--config", str(DEFAULT_CONFIG),
            "process",
            "--variables", "MergedReflectivityQCComposite_00.50",
            "--process-start", "20201014",
            "--process-end",   "20201015",
            "--dry-run",
        ]

    # %%
    parser = build_parser()
    args   = parser.parse_args()
    cfg    = load_config(args.config)

    zarr_cfg  = cfg["zarr"]
    s3_cfg    = cfg["s3"]
    var_reg   = cfg["variables"]
    freq      = zarr_cfg.get("freq", "5min")

    zarr_path = resolve_path(zarr_cfg["store"])
    orog_path = resolve_path(cfg["grid"]["orography"])

    # ── PROCESS ──────────────────────────────────────────────
    # %%
    if args.subcommand == "process":
        full_start = args.full_start or cfg.get("full_start", "20180101")
        full_end   = args.full_end   or cfg.get("full_end",   "20271231")
        full_dates = build_full_dates(full_start, full_end, freq)

        proc_days = pd.date_range(
            start=pd.to_datetime(args.process_start, format="%Y%m%d"),
            end=pd.to_datetime(args.process_end,   format="%Y%m%d"),
            freq="D",
        )
        proc_days_str = [d.strftime("%Y%m%d") for d in proc_days]
        n_workers     = args.workers or cfg.get("workers", 8)

        # Build target grid once — reused for every file
        query_points, out_shape = load_target_grid(orog_path)

        for product in args.variables:
            if product not in var_reg:
                print(f"[error] '{product}' not found in config variables. "
                      f"Available: {list(var_reg.keys())}")
                continue

            var_cfg  = var_reg[product]
            var_name = var_cfg["var_name"]

            print(f"\n{'='*62}")
            print(f"  product  : {product}")
            print(f"  variable : {var_name}")
            print(f"  days     : {proc_days_str[0]} → {proc_days_str[-1]}  ({len(proc_days_str)})")
            print(f"  store    : {zarr_path}")
            print(f"{'='*62}")

            if not args.dry_run:
                ensure_initialized(
                    zarr_path, full_dates, var_name, var_cfg, orog_path, zarr_cfg
                )

            for day_str in tqdm(proc_days_str, desc=var_name, unit="day"):
                if args.dry_run:
                    print(f"  [dry-run] {day_str} → {var_name}")
                    continue
                process_and_write_day(
                    day_str=day_str,
                    product=product,
                    var_name=var_name,
                    var_cfg=var_cfg,
                    zarr_path=zarr_path,
                    full_dates=full_dates,
                    s3_cfg=s3_cfg,
                    query_points=query_points,
                    out_shape=out_shape,
                    zarr_cfg=zarr_cfg,
                    freq=freq,
                    n_workers=n_workers,
                )

    # ── EXTEND ───────────────────────────────────────────────
    # %%
    elif args.subcommand == "extend":
        extend_time_axis(
            zarr_path=zarr_path,
            new_full_end=args.full_end,
            freq=freq,
            dry_run=args.dry_run,
        )

    # ── CHECK ────────────────────────────────────────────────
    # %%
    elif args.subcommand == "check":
        proc_days = pd.date_range(
            start=pd.to_datetime(args.process_start, format="%Y%m%d"),
            end=pd.to_datetime(args.process_end,   format="%Y%m%d"),
            freq="D",
        )
        for product in args.variables:
            if product not in var_reg:
                print(f"[error] '{product}' not in config. Skipping.")
                continue
            var_name = var_reg[product]["var_name"]
            missing  = find_missing_days(zarr_path, list(proc_days), var_name, freq)
            print(
                f"\n{product}  ({var_name}): "
                f"{len(missing)} missing / {len(proc_days)} total days"
            )
            for d in missing:
                print(f"  {d}")
# %%
