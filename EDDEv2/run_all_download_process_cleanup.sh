#!/bin/bash
# Submit EDDEv2 per-(run_type, source_var, year) download+process(+cleanup) jobs.
#
# For each (run_type, year) in YEARS, submits one
# jobsub_download_process_cleanup.slurm job per source variable in VARS.
# Each job:
#   1. Downloads that var/year's raw files (download_EDDEv2_through_AWS.slurm,
#      unmodified, skips files already present).
#   2. Runs process_and_write_to_zarr.py for that var/year (unmodified).
#   3. If REMOVE_RAW=1 and step 2 reports the year is now NaN-free for the
#      corresponding zarr variable, deletes that var/year's raw files.
#
# REMOVE_RAW=0 (default) keeps all raw files regardless of completeness --
# safe to use to backfill data first, then re-run with REMOVE_RAW=1 once
# you're confident the zarr stores are correct.
#
# Edit VARS / RUN_TYPES / YEARS / REMOVE_RAW below, then run:
#   ./run_all_download_process_cleanup.sh
# or:
#   REMOVE_RAW=1 ./run_all_download_process_cleanup.sh

MAX_PARALLEL=8
JOBNAME=EDDEv2_dpc

# EDDEv2 source variable list (download_EDDEv2_through_AWS.slurm vocabulary).
VARS=(
    pr
    ps
    td
    ts
    ua
    va
    wdirs
    wspds
)

RUN_TYPES=(
    SSP2-4.5
    SSP3-7.0
)

# Per-run-type year-spec: comma-separated list of single years and/or
# "start-end" ranges, e.g. "2025", "2025,2030,2100", "2025-2100".
declare -A YEARS=(
    [SSP2-4.5]="2025-2100"
    [SSP3-7.0]="2025-2100"
)

# 1 = delete raw inputs for a (var, year) once it's confirmed NaN-free.
# 0 (default) = keep raw regardless.
REMOVE_RAW="${REMOVE_RAW:-0}"

throttle() {
    while [ "$(squeue -u "$USER" -h -n "$JOBNAME" | wc -l)" -ge "$MAX_PARALLEL" ]; do
        echo "Reached $MAX_PARALLEL queued/running $JOBNAME jobs. Waiting..."
        sleep 30
    done
}

# Expand a year-spec ("2025", "2025,2030,2100", "2025-2100", or a mix) into
# individual years on stdout, separated by spaces.
expand_years() {
    local spec="$1"
    local -a years=()
    local token start end y
    IFS=',' read -ra tokens <<< "$spec"
    for token in "${tokens[@]}"; do
        token="${token// /}"
        if [[ "$token" =~ ^([0-9]+)-([0-9]+)$ ]]; then
            start="${BASH_REMATCH[1]}"
            end="${BASH_REMATCH[2]}"
            for ((y = start; y <= end; y++)); do
                years+=("$y")
            done
        elif [[ "$token" =~ ^[0-9]+$ ]]; then
            years+=("$token")
        else
            echo "WARNING: ignoring invalid year token '$token' in spec '$spec'" >&2
        fi
    done
    echo "${years[@]}"
}

for RUN_TYPE in "${RUN_TYPES[@]}"; do
    if [[ -z "${YEARS[$RUN_TYPE]+x}" ]]; then
        echo "WARNING: no YEARS entry for run_type $RUN_TYPE, skipping" >&2
        continue
    fi

    for YEAR in $(expand_years "${YEARS[$RUN_TYPE]}"); do
        for VAR in "${VARS[@]}"; do
            throttle
            echo "Submitting: $RUN_TYPE $VAR $YEAR (REMOVE_RAW=$REMOVE_RAW)"
            REMOVE_RAW="$REMOVE_RAW" sbatch jobsub_download_process_cleanup.slurm \
                "$RUN_TYPE" "$VAR" "$YEAR"
            sleep 1
        done
    done
done

echo "=============================================="
echo "All jobs submitted!"
echo "=============================================="
