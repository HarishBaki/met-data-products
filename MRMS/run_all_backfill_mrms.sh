#!/bin/bash
# Submit per-(product, year) backfill jobs for the 6 Tier-2/3 MRMS variables
# that have never been written to MRMS.zarr:
#   EchoTop_18_00.50, VIL_00.50, VIL_Density_00.50, PrecipRate_00.00,
#   PrecipFlag_00.00, MergedAzShear0to2kmAGL_00.50
#
# NOAA's S3 archive for these products starts 2020-10-14 (matches the
# retired cropped_NYS pipeline's start date). Each job runs
# `process_mrms.py process --variables <product> --process-start <chunk_start>
# --process-end <chunk_end>` for one calendar year (clipped to
# BACKFILL_START/BACKFILL_END) via jobsub_backfill_mrms.slurm.
# ensure_initialized() adds each variable to the existing zarr (NaN-filled)
# on its first write -- no separate init step needed.
#
# process_mrms.py is idempotent (skips days already in the zarr via
# check_day_in_zarr), so this script is safe to re-run, e.g. to resume any
# year-jobs that hit their --time limit, or to pick up newer days by raising
# BACKFILL_END.
#
# Usage:
#   ./run_all_backfill_mrms.sh
#   BACKFILL_END=20251231 ./run_all_backfill_mrms.sh

MAX_PARALLEL=6
JOBNAME=MRMS_backfill

BACKFILL_START="20201014"
BACKFILL_END="${BACKFILL_END:-$(date -d yesterday +%Y%m%d)}"

PRODUCTS=(
    EchoTop_18_00.50
    VIL_00.50
    VIL_Density_00.50
    PrecipRate_00.00
    PrecipFlag_00.00
    MergedAzShear0to2kmAGL_00.50
)

throttle() {
    while [ "$(squeue -u "$USER" -h -n "$JOBNAME" | wc -l)" -ge "$MAX_PARALLEL" ]; do
        echo "Reached $MAX_PARALLEL queued/running $JOBNAME jobs. Waiting..."
        sleep 30
    done
}

START_YEAR="${BACKFILL_START:0:4}"
END_YEAR="${BACKFILL_END:0:4}"

for PRODUCT in "${PRODUCTS[@]}"; do
    for ((YEAR = START_YEAR; YEAR <= END_YEAR; YEAR++)); do
        CHUNK_START="${YEAR}0101"
        CHUNK_END="${YEAR}1231"
        [ "$CHUNK_START" -lt "$BACKFILL_START" ] && CHUNK_START="$BACKFILL_START"
        [ "$CHUNK_END" -gt "$BACKFILL_END" ] && CHUNK_END="$BACKFILL_END"

        throttle
        echo "Submitting: $PRODUCT $CHUNK_START -> $CHUNK_END"
        sbatch jobsub_backfill_mrms.slurm "$PRODUCT" "$CHUNK_START" "$CHUNK_END"
        sleep 1
    done
done

echo "=============================================="
echo "All backfill jobs submitted!"
echo "=============================================="
