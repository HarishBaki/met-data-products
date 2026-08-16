# %%
"""
Compute global min, max, mean, and std for all variables in a region's
auxiliary_data file (no time dimension; reduces over spatial dims). Used by
DL_downscaling's aux normalization (train.py's aux_transform, which reads
this from paths.auxiliary_stats in configs/regions/{region}.yaml).

Reads paths.auxiliary_data directly from climate-dl-downscaling's own region
config (cross-repo -- that's the actual source of truth for this path, the
same way paths.urma_orog is), not from anything in this repo's own region
configs.

Usage:
    python compute_stats.py --region New_Mexico
"""

import argparse
import sys
from pathlib import Path

import xarray as xr
import yaml

BOOTSTRAP_ROOT = Path(__file__).resolve().parents[1]
if str(BOOTSTRAP_ROOT) not in sys.path:
  sys.path.insert(0, str(BOOTSTRAP_ROOT))

from repo_utils import find_repo_root, load_region_vars

PROJECT_DIR = find_repo_root(__file__)
DL_DOWNSCALING_REPO = PROJECT_DIR.parent / "climate-dl-downscaling"


def main():
  parser = argparse.ArgumentParser(description=__doc__)
  parser.add_argument("--region", required=True, help="e.g. New_York, New_Mexico")
  args = parser.parse_args()

  region_tag = load_region_vars(args.region)["region_tag"]

  dl_region_cfg_path = DL_DOWNSCALING_REPO / "configs" / "regions" / f"{args.region}.yaml"
  with open(dl_region_cfg_path) as f:
    dl_region_cfg = yaml.safe_load(f)
  data_path = dl_region_cfg["paths"].get("auxiliary_data")
  if not data_path:
    raise ValueError(
        f"{dl_region_cfg_path}'s paths.auxiliary_data is not set -- nothing to compute stats from."
    )
  out_path = PROJECT_DIR / "Geomorpho90m" / f"geomorpho90m_stats_{region_tag}.nc"

  print(f"Opening auxiliary_data: {data_path}")
  ds = xr.open_dataset(data_path)
  stats = {}
  for var in ds.data_vars:
    print(f"Computing stats for {var}")
    da = ds[var]
    dims = tuple(da.dims)  # reduce over all dims (spatial only)
    stats[f"{var}_min"] = da.min(dim=dims, skipna=True)
    stats[f"{var}_max"] = da.max(dim=dims, skipna=True)
    stats[f"{var}_mean"] = da.mean(dim=dims, skipna=True)
    stats[f"{var}_std"] = da.std(dim=dims, skipna=True)

  ds_out = xr.Dataset(stats)
  ds_out.attrs["source"] = str(data_path)
  ds_out.to_netcdf(str(out_path))
  print(f"Wrote spatial stats to {out_path}")


# %%
if __name__ == "__main__":
  main()
