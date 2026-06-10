#!/bin/bash
# Submit Ouranos per-(index, var, year) processing jobs with throttling.
#
# For each catalog index: submits a "init" job (creates the zarr store skeleton,
# fast/no-op if already done), then for each year submits the 7 download-var jobs
# (t2m sp sh2 tp rh2 u10 v10) depending on init, then si10/wdir10 depending on
# that year's u10+v10 jobs.
#
# Edit INDICES / YEARS below, then run:
#   ./run_all_process_and_write_to_zarr.sh

MAX_PARALLEL=7
JOBNAME=process_ouranos
SLURM_SCRIPT=process_and_write_to_zarr.slurm

# Catalog row indices to process.
INDICES=(0 5)

# Per-index year-spec: comma-separated list of single years and/or "start-end"
# ranges, e.g. "2025", "2025,2030,2100", "2018-2020", or "2018-2020,2025,2100".
# Indices in INDICES with no entry here are skipped.
declare -A YEARS=(
    [0]="2018-2020"
    [5]="2025"
)

DOWNLOAD_VARS=(t2m sp sh2 tp rh2 u10 v10)

throttle() {
    while [ "$(squeue -u "$USER" -h -n "$JOBNAME" | wc -l)" -ge "$MAX_PARALLEL" ]; do
        echo "Reached $MAX_PARALLEL queued/running $JOBNAME jobs. Waiting..."
        sleep 30
    done
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

    for year in $(expand_years "${YEARS[$idx]}"); do
        u10_id=""
        v10_id=""
        for var in "${DOWNLOAD_VARS[@]}"; do
            throttle
            jid=$(sbatch --parsable --dependency=afterok:"$init_id" "$SLURM_SCRIPT" "$idx" "$var" "$year")
            echo "Submitted: index=$idx var=$var year=$year job=$jid"
            [[ "$var" == "u10" ]] && u10_id=$jid
            [[ "$var" == "v10" ]] && v10_id=$jid
        done

        for var in si10 wdir10; do
            throttle
            jid=$(sbatch --parsable --dependency=afterok:"$u10_id":"$v10_id" "$SLURM_SCRIPT" "$idx" "$var" "$year")
            echo "Submitted: index=$idx var=$var year=$year job=$jid"
        done
    done
done

echo "=============================================="
echo "All jobs submitted!"
echo "=============================================="
