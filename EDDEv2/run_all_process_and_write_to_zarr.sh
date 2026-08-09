#!/bin/bash

# ==============================
# CONFIGURATION
# ==============================
MAX_PARALLEL=3        # its-head: 3 concurrent jobs

# Region to process (e.g. New_York, New_Mexico). Export before running, e.g.:
#   REGION=New_Mexico ./run_all_process_and_write_to_zarr.sh
REGION="${REGION:?REGION must be set (e.g. REGION=New_Mexico) -- no default, to avoid silently running against the wrong region}"
export REGION

# EDDEv2 variable list
VARS=(
    si10    # 10 m wind speed
    t2m     # 2 m air temperature
    sp      # surface pressure
    d2m     # 2 m dew point temperature
    u10     # 10 m eastward wind
    v10     # 10 m northward wind
    wdir10  # 10 m wind direction
    tp      # total precipitation
)

# EDDEv2 run types
RUN_TYPES=(
    Historical
    SSP2-4.5
    SSP3-7.0
)

# ==============================
# PRE-INIT (serial, one job, before any parallel submissions)
# ==============================
# Pre-initialize the output zarr store/skeleton for every (run_type, var) via
# a single serial SLURM job before any parallel jobs run -- otherwise
# multiple jobs targeting the same not-yet-existing store can simultaneously
# race to create it, corrupting it. See process_and_write_to_zarr.py
# --init-only.
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

INIT_ENTRIES=()
for RUN_TYPE in "${RUN_TYPES[@]}"; do
    for VAR in "${VARS[@]}"; do
        INIT_ENTRIES+=("${RUN_TYPE}:${VAR}")
    done
done
init_id=$(sbatch --parsable jobsub_process_and_write_to_zarr_init.slurm "$REGION" "${INIT_ENTRIES[@]}")
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
for RUN_TYPE in "${RUN_TYPES[@]}"; do
    if [ "$RUN_TYPE" = "Historical" ]; then
        PROCESS_START="1985-01"
        PROCESS_END="2014-12"
    else
        PROCESS_START="2025-01"
        PROCESS_END="2099-12"
    fi

    for VAR in "${VARS[@]}"; do

        # Throttle against this user's TOTAL job count on whichever cluster this runs on --
        # not just jobs named EDDEv2 (the submit cap is shared across every product/job
        # name this user submits) and deliberately not QOS-filtered (--qos=freetier is
        # dgx-specific; a QOS name that doesn't exist on another cluster, e.g. its-head,
        # would silently match zero jobs and never throttle at all).
        while [ "$(squeue -u "$USER" -h | wc -l)" -ge "$MAX_PARALLEL" ]; do
            echo "Reached MAX_PARALLEL=${MAX_PARALLEL} jobs. Waiting..."
            sleep 30
        done

        echo "Submitting: $RUN_TYPE $VAR  $PROCESS_START to $PROCESS_END"
        sbatch jobsub_process_and_write_to_zarr.slurm \
            "$VAR" \
            "$PROCESS_START" \
            "$PROCESS_END" \
            "$RUN_TYPE"

        sleep 1   # small delay for SLURM responsiveness

    done
done

echo "=============================================="
echo "All jobs submitted!"
echo "=============================================="
