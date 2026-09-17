# %%
#!/usr/bin/env python
"""Compute per-variable 1st-99th percentiles over a date range of a region's URMA
zarr store -- used by DL_downscaling's InverseWeightedLoss (see stochastic_cgan.yaml's
training.loss), which reads this from paths.urma_percentiles in
configs/regions/{region}.yaml.

Output is written under {data_root}/URMA_{region_tag}/ (the same
Climate-Downscaling_data tree every other processed product for this region
already lives in) -- NOT under this repo. See dataloader.py's
resolve_or_compute_urma_percentiles() for the caller that derives --start/--end
from training.train_dates_range and invokes this script automatically.

Speed fix (2026-09), three rounds -- all measured directly on a 6-year/10-variable
New_Mexico slice, 16-CPU/64G SLURM allocation:
1. Removed chunks="auto" (dask heuristic independent of the store's real
   on-disk chunking): no measurable win for this script on its own (~23min
   either way). Kept anyway since it's free and correct.
2. dask.distributed LocalCluster (one thread per worker PROCESS, sequential
   over variables within a single dask graph): looked promising on a small
   1-year/1-variable probe (2.95x) but at REAL scale this was a clear net
   LOSS -- 31m52s vs the plain-threaded 24m47s baseline. Dask's graph-
   serialization cost to remote processes dominates once the graph is large
   (thousands of chunks x 10 variables); the small-scale probe didn't capture
   that cost because its graph was 10x smaller. Reverted.
3. The actual fix: concurrent OS-level SUBPROCESSES, one per variable, each
   running plain dask (no distributed Client, no graph shipped anywhere --
   the graph never leaves the process that built it). All 10 subprocesses
   share the job's CPU budget via normal OS scheduling instead of dask's
   task scheduler. Measured: 871.4s (14m31s) for all 10 variables run
   concurrently vs 24m47s (1487s) sequential -- a real 1.7x wall-clock win,
   with each variable individually taking ~850s under 10-way contention
   (vs. 165s fully isolated) confirming the CPUs are genuinely the limit,
   not scheduling overhead. An eager-full-.load()-then-numpy rewrite was
   attempted separately and reverted -- URMA_NMS's native grid is 512x480
   (not the 103x96 LR5x5-downsampled grid used elsewhere), so even a single
   4-year variable slice is ~34GB; loading multiple variables at once is not
   memory-safe. Subprocess concurrency gets the win without that risk -- each
   worker is still out-of-core, just a separate OS process per variable.

Usage:
    python compute_percentiles.py --region New_Mexico --start 2018-01-01 --end 2023-12-31
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

PERCENTILES = np.arange(1, 101, dtype=np.int32)


def _compute_worker_variable(region, start, end, var, worker_out):
  region_vars = load_region_vars(region)
  data_root = region_vars["data_root"]
  region_tag = region_vars["region_tag"]
  zarr_store = f"{data_root}/URMA_{region_tag}/URMA_{region_tag}.zarr"

  ds = xr.open_zarr(zarr_store, consolidated=False)
  da = ds.sel(time=slice(start, end))[var]
  if "time" not in da.dims:
    raise ValueError(f"{var} has no 'time' dimension; cannot compute percentiles over time.")

  q = PERCENTILES / 100.0
  t0 = time.perf_counter()
  q_da = da.quantile(q, dim="time", skipna=True).rename({"quantile": "percentile"})
  q_da = q_da.assign_coords(percentile=PERCENTILES).compute()
  print(f"  [{var}] done in {time.perf_counter()-t0:.1f}s", flush=True)

  worker_out = Path(worker_out)
  q_da.to_dataset(name=var).to_netcdf(str(worker_out))


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
  out_path = out_dir / f"urma_percentiles_{region_tag}_{args.start[:4]}_{args.end[:4]}.nc"
  tmp_path = out_path.with_suffix(out_path.suffix + ".tmp")

  print(f"Opening Zarr store: {zarr_store}")
  ds = xr.open_zarr(zarr_store, consolidated=False)
  data_vars = list(ds.data_vars)
  print(f"Variables: {data_vars}")

  out_dir.mkdir(parents=True, exist_ok=True)
  worker_tmp_paths = {
    var: out_dir / f".urma_percentiles_worker_{region_tag}_{args.start[:4]}_{args.end[:4]}_{var}.nc.tmp"
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
  percentiles = {}
  for var in data_vars:
    with xr.open_dataset(worker_tmp_paths[var]) as wds:
      percentiles[var] = wds[var].load()

  ds_out = xr.Dataset(percentiles)
  ds_out.attrs["source"] = zarr_store
  ds_out.attrs["time_range"] = f"{args.start} to {args.end}"
  ds_out.to_netcdf(str(tmp_path))
  tmp_path.rename(out_path)  # atomic -- no reader ever sees a partial file

  for var in data_vars:
    worker_tmp_paths[var].unlink(missing_ok=True)

  print(f"Wrote percentiles to {out_path}")


# %%
if __name__ == "__main__":
  main()

# %%
