# %%
#!/usr/bin/env python
"""Compute per-variable min/max/mean/std (plus a log-transformed variant for tp)
over a date range of a region's URMA zarr store -- used by DL_downscaling's
input/target normalization (see dataloader.py's build_transform(), which reads
this file from paths.urma_stats in configs/regions/{region}.yaml).

Output is written under {data_root}/URMA_{region_tag}/ (the same
Climate-Downscaling_data tree every other processed product for this region
already lives in) -- NOT under this repo, which is scripts + small static/raw
reference files only. See dataloader.py's resolve_or_compute_urma_stats() for
the caller that derives --start/--end from training.train_dates_range and
invokes this script automatically when the matching file doesn't exist yet.

Speed fix (2026-09), three rounds -- see compute_percentiles.py's docstring for
the full methodology and the real-scale timing table (6-year/10-variable probes
on a 16-CPU/64G SLURM allocation). Short version: removing chunks="auto" gave a
modest ~10% win (15m20s -> 13m54s); a dask.distributed LocalCluster looked good
on a small-scale probe but was a net LOSS at real scale (13m54s -> 22m30s,
graph-serialization overhead); concurrent OS-level subprocesses (one per
variable, plain dask, no distributed Client) is the approach that actually
wins, since stats (min/max/mean/std) are cheaper per-variable than percentiles
and split just as cleanly across the store's per-variable layout.

Usage:
    python compute_stats.py --region New_Mexico --start 2018-01-01 --end 2023-12-31
    # Optional: --max-concurrent N (default: one subprocess per variable, i.e.
    # fully concurrent -- lower this if a store ever has many more variables
    # than the allocation has CPUs for)

Internal: this script re-invokes itself as `--worker-var VAR --worker-out PATH`
to compute a single variable in an isolated subprocess; that mode is not meant
to be called directly.
"""
import argparse
import subprocess
import sys
import time
from pathlib import Path

import xarray as xr
import numpy as np

BOOTSTRAP_ROOT = Path(__file__).resolve().parents[1]
if str(BOOTSTRAP_ROOT) not in sys.path:
  sys.path.insert(0, str(BOOTSTRAP_ROOT))

from repo_utils import load_region_vars


def _compute_worker_variable(region, start, end, var, worker_out):
  region_vars = load_region_vars(region)
  data_root = region_vars["data_root"]
  region_tag = region_vars["region_tag"]
  zarr_store = f"{data_root}/URMA_{region_tag}/URMA_{region_tag}.zarr"

  ds = xr.open_zarr(zarr_store, consolidated=False)
  da = ds.sel(time=slice(start, end))[var]
  dims = tuple(dim for dim in da.dims)  # reduce over all dims

  t0 = time.perf_counter()
  stats = {
    f"{var}_min": da.min(dim=dims, skipna=True),
    f"{var}_max": da.max(dim=dims, skipna=True),
    f"{var}_mean": da.mean(dim=dims, skipna=True),
    f"{var}_std": da.std(dim=dims, skipna=True),
  }
  if var == "tp":
    da_log = np.log10(1.0 + da)
    prefix = f"log_{var}"
    stats[f"{prefix}_min"] = da_log.min(dim=dims, skipna=True)
    stats[f"{prefix}_max"] = da_log.max(dim=dims, skipna=True)
    stats[f"{prefix}_mean"] = da_log.mean(dim=dims, skipna=True)
    stats[f"{prefix}_std"] = da_log.std(dim=dims, skipna=True)

  ds_out = xr.Dataset(stats).compute()
  print(f"  [{var}] done in {time.perf_counter()-t0:.1f}s", flush=True)
  ds_out.to_netcdf(str(Path(worker_out)))


def main():
  parser = argparse.ArgumentParser(description=__doc__)
  parser.add_argument("--region", required=True, help="e.g. New_York, New_Mexico")
  parser.add_argument("--start", default="2018-01-01")
  parser.add_argument("--end", default="2023-12-31")
  parser.add_argument("--max-concurrent", type=int, default=None,
                       help="Max concurrent per-variable subprocesses (default: all at once)")
  # Internal worker mode -- see module docstring.
  parser.add_argument("--worker-var", default=None, help=argparse.SUPPRESS)
  parser.add_argument("--worker-out", default=None, help=argparse.SUPPRESS)
  args = parser.parse_args()

  if args.worker_var:
    _compute_worker_variable(args.region, args.start, args.end, args.worker_var, args.worker_out)
    return

  region_vars = load_region_vars(args.region)
  data_root = region_vars["data_root"]
  region_tag = region_vars["region_tag"]
  zarr_store = f"{data_root}/URMA_{region_tag}/URMA_{region_tag}.zarr"
  out_dir = Path(data_root) / f"URMA_{region_tag}"
  out_path = out_dir / f"urma_stats_{region_tag}_{args.start[:4]}_{args.end[:4]}.nc"
  tmp_path = out_path.with_suffix(out_path.suffix + ".tmp")

  print(f"Opening Zarr store: {zarr_store}")
  ds = xr.open_zarr(zarr_store, consolidated=False)
  data_vars = list(ds.data_vars)
  print(f"Variables: {data_vars}")

  out_dir.mkdir(parents=True, exist_ok=True)
  worker_tmp_paths = {
    var: out_dir / f".urma_stats_worker_{region_tag}_{args.start[:4]}_{args.end[:4]}_{var}.nc.tmp"
    for var in data_vars
  }

  max_concurrent = args.max_concurrent or len(data_vars)
  print(f"Launching {len(data_vars)} per-variable subprocesses (max {max_concurrent} concurrent)")
  _tcompute = time.perf_counter()

  pending = list(data_vars)
  running = {}  # var -> Popen
  while pending or running:
    while pending and len(running) < max_concurrent:
      var = pending.pop(0)
      running[var] = subprocess.Popen([
        sys.executable, str(Path(__file__).resolve()),
        "--region", args.region, "--start", args.start, "--end", args.end,
        "--worker-var", var, "--worker-out", str(worker_tmp_paths[var]),
      ])
    for var in list(running):
      p = running[var]
      ret = p.poll()
      if ret is not None:
        if ret != 0:
          for other in running.values():
            other.terminate()
          raise RuntimeError(f"Worker for variable '{var}' failed with exit code {ret}")
        del running[var]
    if running:
      time.sleep(1)

  print(f"All variables computed in {time.perf_counter()-_tcompute:.1f}s, merging output")
  stats = {}
  for var in data_vars:
    with xr.open_dataset(worker_tmp_paths[var]) as wds:
      stats.update({name: wds[name].load() for name in wds.data_vars})

  ds_out = xr.Dataset(stats)
  ds_out.attrs["source"] = zarr_store
  ds_out.attrs["time_range"] = f"{args.start} to {args.end}"
  ds_out.to_netcdf(str(tmp_path))
  tmp_path.rename(out_path)  # atomic -- no reader ever sees a partial file

  for var in data_vars:
    worker_tmp_paths[var].unlink(missing_ok=True)

  print(f"Wrote stats to {out_path}")


# %%
if __name__ == "__main__":
  main()

# %%
