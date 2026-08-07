"""Helpers for resolving paths relative to this repository."""
from __future__ import annotations

import os
import subprocess
from pathlib import Path

import yaml


def find_repo_root(start_path: str | os.PathLike[str] | None = None) -> Path:
    """Return the repository root for a file or working directory."""
    env_root = os.getenv("MET_DATA_PRODUCTS_REPO_ROOT")
    if env_root:
        return Path(env_root).expanduser().resolve()

    base = Path(start_path).resolve() if start_path is not None else Path.cwd().resolve()
    if base.is_file():
        base = base.parent

    try:
        root = subprocess.check_output(
            ["git", "rev-parse", "--show-toplevel"],
            text=True,
            cwd=base,
            stderr=subprocess.DEVNULL,
        ).strip()
    except (FileNotFoundError, subprocess.CalledProcessError):
        root = ""

    if root:
        return Path(root).resolve()

    for parent in [base] + list(base.parents):
        if (parent / ".git").exists() or (parent / "data_utils").is_dir():
            return parent

    return base


def _load_region_config(region: str, repo_root: Path | None = None) -> dict:
    repo_root = repo_root or find_repo_root()
    region_path = repo_root / "configs" / "regions" / f"{region}.yaml"
    if not region_path.is_file():
        raise FileNotFoundError(f"No region config at {region_path}")
    with open(region_path) as f:
        return yaml.safe_load(f)


def load_region_grid(region: str, product: str, repo_root: Path | None = None) -> dict:
    """Load configs/regions/{region}.yaml's grid.<product> entry -- either a
    structured crop ({type, dims, n0, n1, crop: {dim0_start, dim1_start}}) or
    an unstructured cell mask ({type: unstructured, cell_mask_file, ...}).
    See compute_and_write_region_crop.py for how these are produced."""
    cfg = _load_region_config(region, repo_root)
    grid = cfg.get("grid", {})
    if product not in grid:
        raise KeyError(
            f"configs/regions/{region}.yaml has no grid.{product} entry -- "
            f"run compute_and_write_region_crop.py --product {product} ... --update-config first."
        )
    return grid[product]


def load_region_vars(region: str, repo_root: Path | None = None) -> dict:
    """Load configs/regions/{region}.yaml's region-level keys (data_root,
    region_tag, region_id, ...) -- everything except grid/boundary, which
    have their own accessors."""
    cfg = _load_region_config(region, repo_root)
    return {k: v for k, v in cfg.items() if k not in ("grid", "boundary")}
