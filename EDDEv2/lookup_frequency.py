#!/usr/bin/env python3
"""
Resolve which EDDEv2 top-level frequency folder (hourly, 6hourly, daily,
monthly, hifr, static) a given variable actually lives under, by looking it
up in catalog_with_vars.csv (built by discover_vars.py).

This is the single source of truth download_EDDEv2_through_AWS.slurm uses to
derive its S3 prefix and local dest_root - there is no hardcoded fallback
anywhere in that script, specifically so a variable can never silently land
in the wrong frequency folder (which is exactly how psl/zg500 ended up
misplaced under hourly/ before this existed).

Some variable names genuinely exist under more than one frequency with
different meaning (e.g. "pr" at both hourly and hifr; "wspds" at both hourly
and daily, unlike e.g. "wspdsmon" at monthly which got a distinct suffix).
For those, pass --frequency explicitly to disambiguate - it is validated
against the catalog (must actually be one of that variable's real
frequencies), never used to bypass the catalog. There is still no silent
default for the ambiguous case: omitting --frequency when the variable is
ambiguous is an error, not a guess.

Exits nonzero with a clear message, never a guess, if:
  - the catalog file is missing (run discover_vars.py first)
  - the variable isn't in the catalog at all
  - the variable appears under more than one frequency and --frequency
    wasn't given
  - --frequency was given but that variable does not actually exist under
    that frequency in the catalog (catches typos rather than trusting them)

On success, prints exactly the resolved frequency name to stdout and exits 0.

Usage:
    python lookup_frequency.py <var_name> [--catalog PATH]
    python lookup_frequency.py <var_name> --frequency hifr   # disambiguate
"""

import argparse
import csv
from pathlib import Path

DEFAULT_CATALOG = Path(__file__).parent / "catalog_with_vars.csv"


def resolve_frequency(var_name: str, catalog_path: Path, frequency: str | None = None) -> str:
    if not catalog_path.exists():
        raise SystemExit(
            f"ERROR: catalog file not found: {catalog_path}\n"
            f"Run discover_vars.py first to build it."
        )

    matches: set[str] = set()
    with open(catalog_path, newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            vars_ = row["variables"].split(";") if row["variables"] else []
            if var_name in vars_:
                matches.add(row["frequency"])

    if not matches:
        raise SystemExit(
            f"ERROR: '{var_name}' not found in {catalog_path}.\n"
            f"Either the variable name is wrong, or the catalog is stale - "
            f"re-run discover_vars.py to refresh it."
        )

    if frequency is not None:
        if frequency not in matches:
            raise SystemExit(
                f"ERROR: '{var_name}' does not exist under frequency "
                f"'{frequency}' in {catalog_path}. It actually exists under: "
                f"{sorted(matches)}."
            )
        return frequency

    if len(matches) > 1:
        raise SystemExit(
            f"ERROR: '{var_name}' is ambiguous - it appears under multiple "
            f"frequencies in {catalog_path}: {sorted(matches)}. "
            f"Set FREQUENCY explicitly to one of these to disambiguate."
        )
    return matches.pop()


def main():
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("var_name")
    p.add_argument("--catalog", default=str(DEFAULT_CATALOG))
    p.add_argument("--frequency", default=None,
                    help="Explicit frequency, required only if var_name is ambiguous. "
                         "Validated against the catalog, not a bypass.")
    args = p.parse_args()

    freq = resolve_frequency(args.var_name, Path(args.catalog), args.frequency)
    print(freq)


if __name__ == "__main__":
    main()
