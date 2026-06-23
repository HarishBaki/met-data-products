#!/bin/bash
# Submit Ouranos per-(index, var, year) processing jobs with throttling.
#
# For each catalog index: submits a "init" job (creates the zarr store skeleton,
# fast/no-op if already done) and waits for it to finish, then for each year
# submits the 7 download-var jobs (t2m sp sh2 tp rh2 u10 v10), waits for
# u10/v10 to finish, then submits si10/wdir10.
#
# Prerequisites are enforced by polling+waiting rather than --dependency=afterok:
# SLURM purges completed jobs from squeue/sacct after MinJobAge (300s on this
# cluster), and a multi-year submission loop easily exceeds that gap, causing
# "Job dependency problem" failures for every job depending on a purged ID.
#
# Edit INDICES / YEARS / FREQUENCY below, then run:
#   ./run_all_process_and_write_to_zarr.sh
# Or override FREQUENCY / CATALOG via env vars, e.g.:
#   FREQUENCY=day ./run_all_process_and_write_to_zarr.sh

MAX_PARALLEL=7
JOBNAME=process_ouranos
SLURM_SCRIPT=process_and_write_to_zarr.slurm

# Catalog row indices to process.
INDICES=(5)

# Per-index year-spec: comma-separated list of single years and/or "start-end"
# ranges, e.g. "2025", "2025,2030,2100", "2018-2020", or "2018-2020,2025,2100".
# Indices in INDICES with no entry here are skipped.
declare -A YEARS=(
    [0]="2018-2020"
    [5]="2018-2024"
)

# Single frequency for the whole run (1hr/3hr/day/mon). Exported so sbatch
# (and CATALOG, if set) propagate to process_and_write_to_zarr.slurm.
FREQUENCY="${FREQUENCY:-1hr}"
export FREQUENCY
[[ -n "${CATALOG:-}" ]] && export CATALOG

# Download/derived var lists mirror VAR_GROUPS_BY_FREQUENCY in
# process_and_write_to_zarr.py - keep these two in sync by hand.
declare -A DOWNLOAD_VARS_BY_FREQUENCY=(
    [1hr]="t2m sp sh2 tp rh2 u10 v10"
    [3hr]="clwvi hfls mrro prw snw"
    [day]="t2m tasmax tasmin snw"
    [mon]="t2m sp sh2 rh2 u10 v10 tasmax tasmin snw"
)
declare -A DERIVED_VARS_BY_FREQUENCY=(
    [1hr]="si10 wdir10"
    [3hr]=""
    [day]=""
    [mon]="si10 wdir10"
)
DOWNLOAD_VARS=(${DOWNLOAD_VARS_BY_FREQUENCY[$FREQUENCY]})
DERIVED_VARS=(${DERIVED_VARS_BY_FREQUENCY[$FREQUENCY]})

throttle() {
    while [ "$(squeue -u "$USER" -h -n "$JOBNAME" | wc -l)" -ge "$MAX_PARALLEL" ]; do
        echo "Reached $MAX_PARALLEL queued/running $JOBNAME jobs. Waiting..."
        sleep 30
    done
}

# Wait for the given job ID(s) to leave the queue, then verify via sacct that
# they all COMPLETED. Returns 1 immediately if any ID is empty (failed
# submission), or if any job did not COMPLETE.
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
        (( elapsed % 30 == 0 )) && echo "  ... waiting on job(s) $csv (${elapsed}s)"
    done
    local bad
    bad=$(sacct -j "$csv" -X --format=State --noheader 2>/dev/null | tr -d ' ' | grep -vc '^COMPLETED$')
    [ "$bad" -eq 0 ]
}

# Expand a year-spec ("2025", "2025,2030,2100", "2018-2020", or a mix) into
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

for idx in "${INDICES[@]}"; do
    if [[ -z "${YEARS[$idx]+x}" ]]; then
        echo "WARNING: no YEARS entry for index $idx, skipping" >&2
        continue
    fi

    throttle
    init_id=$(sbatch --parsable "$SLURM_SCRIPT" "$idx" init)
    echo "Submitted: index=$idx var=init job=$init_id"

    echo "Waiting for index=$idx init job=$init_id to complete..."
    if ! wait_for_jobs "$init_id"; then
        echo "ERROR: index=$idx init job=$init_id did not complete successfully. Skipping index $idx." >&2
        continue
    fi

    for year in $(expand_years "${YEARS[$idx]}"); do
        u10_id=""
        v10_id=""
        for var in "${DOWNLOAD_VARS[@]}"; do
            throttle
            jid=$(sbatch --parsable "$SLURM_SCRIPT" "$idx" "$var" "$year")
            echo "Submitted: index=$idx var=$var year=$year job=$jid"
            [[ "$var" == "u10" ]] && u10_id=$jid
            [[ "$var" == "v10" ]] && v10_id=$jid
        done

        if [[ ${#DERIVED_VARS[@]} -eq 0 ]]; then
            continue
        fi

        echo "Waiting for index=$idx year=$year u10/v10 jobs ($u10_id, $v10_id) to complete..."
        if ! wait_for_jobs "$u10_id" "$v10_id"; then
            echo "ERROR: index=$idx year=$year u10/v10 jobs did not complete successfully. Skipping ${DERIVED_VARS[*]} for this year." >&2
            continue
        fi

        for var in "${DERIVED_VARS[@]}"; do
            throttle
            jid=$(sbatch --parsable "$SLURM_SCRIPT" "$idx" "$var" "$year")
            echo "Submitted: index=$idx var=$var year=$year job=$jid"
        done
    done
done

echo "=============================================="
echo "All jobs submitted!"
echo "=============================================="
