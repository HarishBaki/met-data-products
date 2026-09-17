#!/bin/bash

# ==============================
# CONFIGURATION
# ==============================
# Same throttle convention as run_all_process_and_write_to_zarr.sh. Override
# without editing the file, e.g.:
#   MAX_PARALLEL=4 REGION=New_Mexico ./run_rechunk_urma.sh
MAX_PARALLEL="${MAX_PARALLEL:-7}"

REGION="${REGION:?REGION must be set (e.g. REGION=New_Mexico) -- no default, to avoid silently running against the wrong region}"
export REGION

STORE_SUFFIX="${STORE_SUFFIX:-_rechunk24h}"
export STORE_SUFFIX

# URMA variable list -- must match jobsub_rechunk_urma_init.slurm's var list
# (the sibling skeleton must already have all of these initialized).
VARS=(
    si10    # 10 m wind speed
    i10fg   # 10 m wind gust
    t2m     # 2 m air temperature
    sp      # surface pressure
    d2m     # 2 m dew point temperature
    u10     # 10 m eastward wind
    v10     # 10 m northward wind
    sh2     # 2 m specific humidity
    wdir10  # 10 m wind direction
    tp      # total precipitation
)

# Real-data year range per variable (see nan_times/ -- tp's precip filename
# convention changed 2018-11-26, so no real tp data exists before 2018).
START_YEAR=2014
END_YEAR=2025
TP_START_YEAR=2018

# ==============================
# PRE-INIT CHECK
# ==============================
# The sibling skeleton (URMA_NMS${STORE_SUFFIX}.zarr) must already exist --
# built once, serially, via jobsub_rechunk_urma_init.slurm -- before this
# script's parallel copy jobs run, for the same store-creation-race reason
# documented in run_all_process_and_write_to_zarr.sh. This script does not
# submit that step itself (run it once by hand first); it only checks the
# store is present before fanning out.
DATA_ROOT_CHECK=$(python3 -c "
import sys
sys.path.insert(0, '..')
from repo_utils import load_region_vars
rv = load_region_vars('$REGION')
print(rv['data_root'])
")
SIBLING_STORE="${DATA_ROOT_CHECK}/URMA_NMS/URMA_NMS${STORE_SUFFIX}.zarr"
if [ ! -d "$SIBLING_STORE" ]; then
    echo "ERROR: sibling store not found at $SIBLING_STORE" >&2
    echo "Run jobsub_rechunk_urma_init.slurm first (see rechunk plan Part 2)." >&2
    exit 1
fi
echo "Sibling store found: $SIBLING_STORE -- proceeding with parallel copy."

# ==============================
# MAIN LOOP
# ==============================
mkdir -p slurmout
all_ids=()
for VAR in "${VARS[@]}"; do
    var_start_year=$START_YEAR
    if [ "$VAR" = "tp" ]; then
        var_start_year=$TP_START_YEAR
    fi
    for YEAR in $(seq $var_start_year $END_YEAR); do

        while [ "$(squeue -u "$USER" -h | wc -l)" -ge "$MAX_PARALLEL" ]; do
            echo "Reached MAX_PARALLEL=${MAX_PARALLEL} jobs. Waiting..."
            sleep 30
        done

        echo "Submitting: $VAR $YEAR"
        jid=$(sbatch --parsable jobsub_rechunk_urma_copy.slurm "$VAR" "$YEAR")
        all_ids+=("$jid")

        sleep 1

    done
done

echo "=============================================="
echo "All ${#all_ids[@]} rechunk-copy jobs submitted!"
echo "=============================================="
echo "Job IDs: ${all_ids[*]}"
echo "Check slurmout/URMA-rechunk-copy-*.out and rechunk_failed_jobs.log for results."
echo "Once all jobs are COMPLETED, run the validation steps in the rechunk plan"
echo "(Part 4) before any cutover (Part 5)."
