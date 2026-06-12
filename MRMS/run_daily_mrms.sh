#!/bin/bash
# Keep the 2 Tier-1 MRMS variables (Reflectivity_composite,
# Reflectivity_lowest) current in /network/rit/lab/basulab/Projects/WISER/DATA/MRMS.zarr.
#
# process_mrms.py's `process` subcommand skips any day already present in
# the Zarr store (check_day_in_zarr), so this script is idempotent and safe
# to re-run on a recurring cadence (cron `sbatch jobsub_daily_mrms.slurm`,
# or run by hand). Each run re-scans from PROCESS_START through "yesterday",
# which both closes the initial 2025-08-27 backfill gap and self-heals any
# day a previous run failed to write.
#
# Usage:
#   bash run_daily_mrms.sh
#   PROCESS_START=20260101 bash run_daily_mrms.sh   # narrow the re-scan once
#                                                     # the initial gap is closed

set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")"

PROCESS_START="${PROCESS_START:-20250827}"
PROCESS_END="${PROCESS_END:-$(date -d yesterday +%Y%m%d)}"

VARIABLES=(
    MergedReflectivityQCComposite_00.50
    MergedReflectivityAtLowestAltitude_00.50
)

echo "=========================================================="
echo "MRMS daily update"
echo "Variables: ${VARIABLES[*]}"
echo "Range:     $PROCESS_START -> $PROCESS_END"
echo "=========================================================="

python process_mrms.py process \
    --variables "${VARIABLES[@]}" \
    --process-start "$PROCESS_START" \
    --process-end "$PROCESS_END"
