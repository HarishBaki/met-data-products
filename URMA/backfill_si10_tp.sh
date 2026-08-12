#!/bin/bash

set -euo pipefail

# Targeted resubmission for the 2 URMA variables flagged by compare_nan_times.py
# as having real, NM-specific gaps (not explained by the same timestamps in New
# York) -- si10 (2018 only) and tp (scattered across 2018-2025). Submits only the
# affected (var, year) jobs rather than the full run_all_process_and_write_to_zarr.sh
# sweep, since every other var/year is already confirmed complete or fully explained
# by the shared source archive.
#
# Now that URMA/process_and_write_to_zarr.py has a real has_missing_data()-based
# completeness check (was previously always exiting 0 regardless of outcome), these
# jobs will actually reprocess and backfill the specific stuck partial-days
# instead of silently re-skipping them.
#
# Usage:
#   REGION=New_Mexico ./backfill_si10_tp.sh
#   MAX_PARALLEL=3 REGION=New_Mexico ./backfill_si10_tp.sh

MAX_PARALLEL="${MAX_PARALLEL:-5}"
REGION="${REGION:?REGION must be set (e.g. REGION=New_Mexico) -- no default, to avoid silently running against the wrong region}"
export REGION

wait_for_slot() {
    while [ "$(squeue -u "$USER" -h | wc -l)" -ge "$MAX_PARALLEL" ]; do
        echo "Reached MAX_PARALLEL=${MAX_PARALLEL} jobs. Waiting..."
        sleep 30
    done
}

for YEAR in 2018 2019 2020 2021 2022 2023 2024 2025; do
    wait_for_slot
    echo "Submitting: tp $YEAR"
    sbatch jobsub_process_and_write_to_zarr.slurm tp "$YEAR"
    sleep 1
done

wait_for_slot
echo "Submitting: si10 2018"
sbatch jobsub_process_and_write_to_zarr.slurm si10 2018

echo "=============================================="
echo "All backfill jobs submitted. Check URMA/failed_jobs.log for failures (no sacct needed)."
echo "=============================================="
