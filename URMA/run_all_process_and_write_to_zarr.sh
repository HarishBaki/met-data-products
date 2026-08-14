#!/bin/bash

# ==============================
# CONFIGURATION
# ==============================
# freetier QOS cap; unfiltered count below absorbs any non-freetier jobs (e.g.
# vscode-dgx) automatically. Override without editing the file, e.g.:
#   MAX_PARALLEL=4 REGION=New_Mexico ./run_all_process_and_write_to_zarr.sh
MAX_PARALLEL="${MAX_PARALLEL:-8}"

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

# Same throttle as the main loop below -- without it, the init submission assumes a
# free slot always exists, which is only true if nothing else is using the shared quota.
# In a sequential product chain, the previous product's own jobs can still be running
# right when this fires, causing QOSMaxSubmitJobPerUserLimit on the init call itself
# (confirmed in production: HRRR finishing its submission loop while its own jobs were
# still running left no room for ERA5-ARCO's and ICON's init jobs immediately after).
while [ "$(squeue -u "$USER" -h | wc -l)" -ge "$MAX_PARALLEL" ]; do
    echo "Reached MAX_PARALLEL=${MAX_PARALLEL} jobs. Waiting..."
    sleep 30
done
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
all_ids=()
for VAR in "${VARS[@]}"; do
    for YEAR in $(seq $START_YEAR $END_YEAR); do

        # Throttle against this user's TOTAL job count on whichever cluster this runs on --
        # not just jobs named URMA (the submit cap is shared across every product/job name
        # this user submits) and deliberately not QOS-filtered: an unfiltered count is
        # always >= any single QOS's count, so it can never let a submission through that
        # SLURM would reject, and it automatically absorbs non-freetier jobs (e.g.
        # vscode-dgx) without needing MAX_PARALLEL manually tuned down to leave room.
        while [ "$(squeue -u "$USER" -h | wc -l)" -ge "$MAX_PARALLEL" ]; do
            echo "Reached MAX_PARALLEL=${MAX_PARALLEL} jobs. Waiting..."
            sleep 30
        done

        echo "Submitting: $VAR  $YEAR"
        jid=$(sbatch --parsable jobsub_process_and_write_to_zarr.slurm "$VAR" "$YEAR")
        all_ids+=("$jid")

        sleep 1   # small delay for SLURM responsiveness

    done
done

echo "=============================================="
echo "All jobs submitted!"
echo "=============================================="

# Consolidate once, after every job above has finished. Blocking poll rather than
# --dependency=afterany: SLURM purges completed jobs from squeue/sacct after
# MinJobAge (300s on this cluster), and this 120-job loop easily exceeds that gap,
# so a dependency string built from early job IDs would reference already-purged
# jobs and fail at submission time (same reasoning as Ouranos's run_all script).
# Not afterok-equivalent either: a single stray failure in a 120-job historical
# backfill shouldn't block consolidating whatever did succeed, so failures here
# are only logged, not treated as fatal -- see consolidate_metadata.py.
if [ "${#all_ids[@]}" -gt 0 ]; then
    echo "Waiting for all ${#all_ids[@]} jobs to finish before consolidating..."
    wait_for_jobs "${all_ids[@]}" || echo "WARNING: not every job COMPLETED -- consolidating anyway."
    mkdir -p slurmout
    finalize_id=$(sbatch --parsable \
        --job-name=consolidate-URMA \
        --output=slurmout/consolidate-URMA-%j.out --error=slurmout/consolidate-URMA-%j.err \
        --time=02:00:00 --cpus-per-task=2 --mem=32G --propagate=NONE \
        --wrap="source /network/rit/lab/basulab/mambaforge/etc/profile.d/conda.sh && conda activate /network/rit/lab/basulab/conda_envs/hb533188/DFSAI && cd .. && python consolidate_metadata.py --product URMA --region $REGION")
    echo "Submitted: consolidate job=$finalize_id"
fi