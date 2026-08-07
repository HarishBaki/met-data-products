#!/bin/bash

# ==============================
# CONFIGURATION
# ==============================
MAX_PARALLEL=7

# Region to process (e.g. New_York, New_Mexico). Export before running, e.g.:
#   REGION=New_Mexico ./run_all_process_and_write_to_zarr.sh
REGION="${REGION:-New_York}"
export REGION

VARS=(
    si10
    tp
    i10fg
    t2m
    sp
    d2m
    u10
    v10
    wdir10
    fsr
)

#PROCESS_START="201001"
#PROCESS_END="202512"

# ==============================
# PRE-INIT (serial, one job, before any parallel submissions)
# ==============================
# Pre-initialize the output zarr store/skeleton for every var via a single
# serial SLURM job before any parallel jobs run -- otherwise multiple jobs
# targeting the same not-yet-existing store can simultaneously race to
# create it, corrupting it. See process_and_write_to_zarr.py --init-only.
wait_for_jobs() {
    local id ids=("$@")
    for id in "${ids[@]}"; do
        [[ -z "$id" ]] && return 1
    done
    local csv
    csv=$(IFS=,; echo "${ids[*]}")
    local elapsed=0
    while [ -n "$(squeue -j "$csv" -h 2>/dev/null)" ]; do
        sleep 10
        elapsed=$((elapsed + 10))
        (( elapsed % 30 == 0 )) && echo "  ... waiting on init job $csv (${elapsed}s)"
    done
    local bad
    bad=$(sacct -j "$csv" -X --format=State --noheader 2>/dev/null | tr -d ' ' | grep -vc '^COMPLETED$')
    [ "$bad" -eq 0 ]
}

init_id=$(sbatch --parsable jobsub_process_and_write_to_zarr_init.slurm "$REGION" "${VARS[@]}")
echo "Submitted: init job=$init_id"
echo "Waiting for init job=$init_id to complete..."
if ! wait_for_jobs "$init_id"; then
    echo "ERROR: init job=$init_id did not complete successfully. Aborting." >&2
    exit 1
fi
echo "Pre-init complete -- safe to fan out in parallel now."

# ==============================
# MAIN LOOP
# ==============================
for VAR in "${VARS[@]}"; do
    for year in {2018..2025}; do
        PROCESS_START="${year}01"
        PROCESS_END="${year}12"
        while [ "$(squeue -u "$USER" -n ICON-DREAM -h | wc -l)" -ge "$MAX_PARALLEL" ]; do
            echo "Reached $MAX_PARALLEL jobs running. Waiting..."
            sleep 30
        done

        echo "Submitting: $VAR  $PROCESS_START to $PROCESS_END"
        sbatch jobsub_process_and_write_to_zarr.slurm \
            "$VAR" \
            "$PROCESS_START" \
            "$PROCESS_END"

        sleep 1
    done
done

echo "=============================================="
echo "All jobs submitted!"
echo "=============================================="
