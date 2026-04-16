"""Helpers for resolving paths relative to this repository."""
from __future__ import annotations

import os
import subprocess
from pathlib import Path


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
