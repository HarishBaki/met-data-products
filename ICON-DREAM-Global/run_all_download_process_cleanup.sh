#!/bin/bash
# Submit ICON-DREAM-Global per-month download+process(+cleanup) jobs.
#
# For each month YYYYMM in [START_YEARMONTH, END_YEARMONTH], submits one
# jobsub_download_process_cleanup.slurm job that:
#   1. Downloads that month's raw .grb files for all 9 source variables
#      (download_icon_dream.slurm, unmodified, skips files already present
#      with the correct size).
#   2. Runs process_and_write_to_zarr.py for all 10 canonical variables for
#      that month (unmodified).
#   3. If REMOVE_RAW=1 and all 10 report the month is now NaN-free, deletes
#      that month's 9 raw .grb files.
#
# REMOVE_RAW=0 (default) keeps all raw files regardless of completeness --
# safe to use to backfill data first, then re-run with REMOVE_RAW=1 once
# you're confident the zarr store is correct.
#
# Edit MAX_PARALLEL / START_YEARMONTH / END_YEARMONTH below, then run:
#   ./run_all_download_process_cleanup.sh
# or:
#   REMOVE_RAW=1 ./run_all_download_process_cleanup.sh

MAX_PARALLEL=7
JOBNAME=ICON-DREAM_dpc

# Months to process, inclusive, YYYYMM.
START_YEARMONTH="202508"
END_YEARMONTH="202512"

# 1 = delete this month's raw .grb files once confirmed NaN-free for all
# canonical variables. 0 (default) = keep raw regardless.
REMOVE_RAW="${REMOVE_RAW:-0}"

# Region to process (e.g. New_York, New_Mexico).
REGION="${REGION:-New_York}"

throttle() {
    while [ "$(squeue -u "$USER" -h -n "$JOBNAME" | wc -l)" -ge "$MAX_PARALLEL" ]; do
        echo "Reached $MAX_PARALLEL queued/running $JOBNAME jobs. Waiting..."
        sleep 30
    done
}

# Same YYYYMM increment logic as download_icon_dream.slurm.
next_month() {
    local y="${1:0:4}"
    local m="${1:4:2}"
    m=$((10#$m + 1))
    if [ "$m" -eq 13 ]; then
        y=$((y + 1))
        m=1
    fi
    printf "%04d%02d" "$y" "$m"
}

cur="$START_YEARMONTH"
while [ "$cur" -le "$END_YEARMONTH" ]; do
    throttle
    echo "Submitting: $cur (REMOVE_RAW=$REMOVE_RAW)"
    REMOVE_RAW="$REMOVE_RAW" REGION="$REGION" sbatch jobsub_download_process_cleanup.slurm "$cur"
    sleep 1
    cur="$(next_month "$cur")"
done

echo "=============================================="
echo "All jobs submitted!"
echo "=============================================="
