#!/bin/bash
#SBATCH --job-name=ICON-DREAM_dpc
#SBATCH --output=slurmout/ICON-DREAM_dpc-%j.out
#SBATCH --error=slurmout/ICON-DREAM_dpc-%j.err
#SBATCH --time=04:00:00
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=32
#SBATCH --mem=200G

mkdir -p slurmout

############################################################
# READ ARGUMENTS PASSED TO SBATCH
############################################################
YYYYMM="$1"

if [ -z "$YYYYMM" ]; then
    echo "ERROR: Usage: sbatch jobsub_download_process_cleanup.slurm <yyyymm>"
    echo "Env: REMOVE_RAW=1 to delete this month's raw .grb files once processing"
    echo "     reports the month is NaN-free for every ALL_VARS entry"
    echo "     (default: keep raw)."
    echo "     FULL_START_YEAR / FULL_END_YEAR optionally pass through to"
    echo "     process_and_write_to_zarr.py's --full-start-year/--full-end-year"
    echo "     (must match whatever the existing zarr store was created with)."
    echo "     REGION (default: New_York) selects configs/regions/{region}.yaml."
    exit 1
fi

REGION="${REGION:?REGION must be set (e.g. REGION=New_Mexico) -- no default, to avoid silently running against the wrong region}"

############################################################
# ACTIVATE ENVIRONMENT
############################################################
source /network/rit/lab/basulab/mambaforge/etc/profile.d/conda.sh
conda activate /network/rit/lab/basulab/conda_envs/hb533188/DFSAI

RAW_ROOT="/network/rit/lab/basulab/RAW_DATA/ICON-DREAM-Global"

# 9 raw source variables (download_icon_dream.slurm vocabulary); the raw
# .grb for each lives at RAW_ROOT/<SRC_VAR>/ICON-DREAM-Global_<yyyymm>_<SRC_VAR>_hourly.grb
SRC_VARS=(T_2M PS TD_2M TOT_PREC WS_10M VMAX_10M Z0 U_10M V_10M)

# 10 canonical zarr variables; wdir10 is derived from U_10M+V_10M and has no
# raw file of its own.
ALL_VARS=(t2m sp d2m tp si10 i10fg fsr u10 v10 wdir10)

EXTRA_ARGS=()
[ -n "${FULL_START_YEAR:-}" ] && EXTRA_ARGS+=(--full-start-year "$FULL_START_YEAR")
[ -n "${FULL_END_YEAR:-}" ] && EXTRA_ARGS+=(--full-end-year "$FULL_END_YEAR")

echo "=========================================================="
echo "ICON-DREAM-Global download -> process -> conditional cleanup"
echo "Month:       $YYYYMM"
echo "Region:      $REGION"
echo "REMOVE_RAW:  ${REMOVE_RAW:-0}"
echo "=========================================================="

############################################################
# 1. Download (existing script, unmodified, scoped to this month; already
#    skips files already present with the correct size)
############################################################
for SRC_VAR in "${SRC_VARS[@]}"; do
    bash download_icon_dream.slurm "$SRC_VAR" "$YYYYMM" "$YYYYMM"
done

############################################################
# 2. Process (existing script, unmodified) - one invocation per canonical
#    variable, all for this month.
############################################################
ALL_OK=1
for VAR in "${ALL_VARS[@]}"; do
    python process_and_write_to_zarr.py \
        --var_name "$VAR" \
        --start-yearmonth "$YYYYMM" \
        --end-yearmonth "$YYYYMM" \
        --region "$REGION" \
        "${EXTRA_ARGS[@]}"
    if [ $? -ne 0 ]; then
        ALL_OK=0
    fi
done

############################################################
# 3. Conditional cleanup: only if REMOVE_RAW=1 and ALL 10 variables report
#    this month is now NaN-free.
############################################################
if [ "${REMOVE_RAW:-0}" = "1" ] && [ "$ALL_OK" -eq 1 ]; then
    echo "All ${#ALL_VARS[@]} variables complete for $YYYYMM; removing raw .grb files"
    for SRC_VAR in "${SRC_VARS[@]}"; do
        rm -fv "${RAW_ROOT}/${SRC_VAR}/ICON-DREAM-Global_${YYYYMM}_${SRC_VAR}_hourly.grb"
    done
else
    echo "Skipping raw cleanup (REMOVE_RAW=${REMOVE_RAW:-0}, all_ok=$ALL_OK)"
fi

[ "$ALL_OK" -eq 1 ] || exit 1
