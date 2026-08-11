#!/bin/bash

set -euo pipefail

# ==========================================================
# CONFIGURATION
# ==========================================================
# Override without editing the file, e.g.:
#   MAX_PARALLEL=3 PRODUCTS="URMA HRRR" REGION=New_Mexico ./run_all_compute_nan_times.sh
MAX_PARALLEL="${MAX_PARALLEL:-8}"

REGION="${REGION:?REGION must be set (e.g. REGION=New_Mexico) -- no default, to avoid silently running against the wrong region}"
export REGION

PRODUCTS_DEFAULT="URMA HRRR EDDEv2 ERA5 ICON-DREAM-Global Ouranos"
read -ra PRODUCTS <<< "${PRODUCTS:-$PRODUCTS_DEFAULT}"

# ==========================================================
# HELPERS
# ==========================================================
wait_for_slot() {
    # Deliberately not QOS-filtered: an unfiltered count is always >= any
    # single QOS's count, so it can never let a submission through that
    # SLURM would reject, and it automatically absorbs non-freetier jobs
    # (e.g. vscode-dgx) without needing MAX_PARALLEL manually tuned down.
    while [ "$(squeue -u "$USER" -h | wc -l)" -ge "$MAX_PARALLEL" ]; do
        echo "Reached MAX_PARALLEL=${MAX_PARALLEL} jobs. Waiting..."
        sleep 30
    done
}

# ==========================================================
# MAIN
# ==========================================================
for PRODUCT in "${PRODUCTS[@]}"; do
    wait_for_slot
    echo "Submitting: $PRODUCT $REGION"
    sbatch jobsub_compute_nan_times.slurm "$PRODUCT" "$REGION"
    sleep 1
done

echo "=============================================="
echo "All nan-times jobs submitted."
echo "=============================================="
