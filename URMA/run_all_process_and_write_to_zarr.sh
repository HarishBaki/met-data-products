#!/bin/bash

# ==============================
# CONFIGURATION
# ==============================
MAX_PARALLEL=7        # Limit: 7 jobs running at once

# Region to process (e.g. New_York, New_Mexico). Export before running, e.g.:
#   REGION=New_Mexico ./run_all_process_and_write_to_zarr.sh
REGION="${REGION:?REGION must be set (e.g. REGION=New_Mexico) -- no default, to avoid silently running against the wrong region}"
export REGION

# URMA variable list
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

# Year range
START_YEAR=2014
END_YEAR=2025

# ==============================
# PRE-INIT (serial, one job, before any parallel submissions)
# ==============================
# Pre-initialize the output zarr store/skeleton for every var via a single
# serial SLURM job before any parallel jobs run -- otherwise multiple jobs
# targeting the same not-yet-existing store can simultaneously race to create
# it, corrupting it (zarr.errors.GroupNotFoundError / "Time coordinate
# mismatch" / stale-handle failures -- observed in production from this exact
# race). See process_and_write_to_zarr.py --init-only.
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
    for YEAR in $(seq $START_YEAR $END_YEAR); do

        # Throttle submissions against the GLOBAL freetier QOS job count (MaxSubmitPU=8),
        # not just jobs named URMA -- the QOS cap is shared across every product/job name
        # this user submits (confirmed in production: two products submitting
        # concurrently, each blind to the other, blew straight through the shared cap
        # even though neither exceeded its own per-name MAX_PARALLEL).
        while [ "$(squeue -u "$USER" --qos=freetier -h | wc -l)" -ge "$MAX_PARALLEL" ]; do
            echo "Reached MAX_PARALLEL=${MAX_PARALLEL} jobs under QOS freetier. Waiting..."
            sleep 30
        done

        echo "Submitting: $VAR  $YEAR"
        sbatch jobsub_process_and_write_to_zarr.slurm "$VAR" "$YEAR"

        sleep 1   # small delay for SLURM responsiveness

    done
done

echo "=============================================="
echo "All jobs submitted!"
echo "=============================================="