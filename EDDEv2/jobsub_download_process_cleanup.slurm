#!/bin/bash
#SBATCH --job-name=EDDEv2_dpc
#SBATCH --output=slurmout/EDDEv2_dpc-%j.out
#SBATCH --error=slurmout/EDDEv2_dpc-%j.err
#SBATCH --time=04:00:00
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=32
#SBATCH --mem=64G
#SBATCH --propagate=NONE

mkdir -p slurmout

############################################################
# READ ARGUMENTS PASSED TO SBATCH
############################################################
RUN_TYPE="$1"
SOURCE_VAR="$2"
YEAR="$3"

if [ -z "$RUN_TYPE" ] || [ -z "$SOURCE_VAR" ] || [ -z "$YEAR" ]; then
    echo "ERROR: Usage: sbatch jobsub_download_process_cleanup.slurm <run_type> <source_var> <year>"
    echo "  run_type:   Historical | SSP2-4.5 | SSP3-7.0"
    echo "  source_var: pr | ps | td | ts | wdirs | wspds | ua | va"
    echo "  year:       e.g. 2031"
    echo "Env: REMOVE_RAW=1 to delete raw inputs for this var/year once processing"
    echo "     reports the full year is NaN-free for the corresponding zarr var"
    echo "     (default: keep raw)."
    echo "     REGION (default: New_York) selects configs/regions/{region}.yaml."
    exit 1
fi

REGION="${REGION:?REGION must be set (e.g. REGION=New_Mexico) -- no default, to avoid silently running against the wrong region}"

############################################################
# ACTIVATE ENVIRONMENT
############################################################
source /network/rit/lab/basulab/mambaforge/etc/profile.d/conda.sh
conda activate DFSAI

############################################################
# source_var -> process_and_write_to_zarr.py --var-name (renamed) lookup.
# Mirrors the FILE_TO_SOURCE + rename_map composition in
# process_and_write_to_zarr.py.
############################################################
declare -A SOURCE_VAR_TO_RENAMED=(
    [pr]=tp
    [ps]=sp
    [td]=d2m
    [ts]=t2m
    [wdirs]=wdir10
    [wspds]=si10
    [ua]=u10
    [va]=v10
)
RENAMED_VAR="${SOURCE_VAR_TO_RENAMED[$SOURCE_VAR]}"
if [ -z "$RENAMED_VAR" ]; then
    echo "ERROR: unknown source_var '$SOURCE_VAR'. Must be one of: ${!SOURCE_VAR_TO_RENAMED[*]}"
    exit 1
fi

# run_type -> raw run-folder year-range, mirrors full_years_range in
# process_and_write_to_zarr.py.
declare -A RUN_FOLDER_YEARS=(
    [Historical]=1985-2014
    [SSP2-4.5]=2025-2100
    [SSP3-7.0]=2025-2100
)
RUN_FOLDER="${RUN_FOLDER_YEARS[$RUN_TYPE]}"
if [ -z "$RUN_FOLDER" ]; then
    echo "ERROR: unknown run_type '$RUN_TYPE'. Must be one of: ${!RUN_FOLDER_YEARS[*]}"
    exit 1
fi

RAW_ROOT="/network/rit/lab/basulab/RAW_DATA/EDDE_V2/hourly/WRF-MPI"

echo "=========================================================="
echo "EDDEv2 download -> process -> conditional cleanup"
echo "Run type:    $RUN_TYPE"
echo "Source var:  $SOURCE_VAR  (-> $RENAMED_VAR)"
echo "Year:        $YEAR"
echo "Region:      $REGION"
echo "REMOVE_RAW:  ${REMOVE_RAW:-0}"
echo "=========================================================="

############################################################
# 1. Download (existing script, unmodified, scoped to this var/year;
#    already skips files already present via s5cmd --no-clobber)
############################################################
VAR_NAME="$SOURCE_VAR" START_YEAR="$YEAR" END_YEAR="$YEAR" \
    bash download_EDDEv2_through_AWS.slurm
DOWNLOAD_STATUS=$?
if [ "$DOWNLOAD_STATUS" -ne 0 ]; then
    echo "WARNING: download step exited $DOWNLOAD_STATUS for $SOURCE_VAR $YEAR"
fi

############################################################
# 2. Process (existing script, unmodified)
############################################################
python process_and_write_to_zarr.py \
    --run-type "$RUN_TYPE" \
    --var-name "$RENAMED_VAR" \
    --process-start "${YEAR}-01" \
    --process-end "${YEAR}-12" \
    --region "$REGION"
PROCESS_STATUS=$?

############################################################
# 3. Conditional cleanup: only if REMOVE_RAW=1 and processing reports the
#    full year is now NaN-free for $RENAMED_VAR.
############################################################
if [ "${REMOVE_RAW:-0}" = "1" ] && [ "$PROCESS_STATUS" -eq 0 ]; then
    YEAR_DIR="${RAW_ROOT}/${RUN_TYPE}/${RUN_FOLDER}/${YEAR}"
    echo "Process reported $RENAMED_VAR complete for $YEAR; removing raw $SOURCE_VAR files in $YEAR_DIR"
    rm -fv ${YEAR_DIR}/${SOURCE_VAR}.*.${YEAR}-*.raw.nc
else
    echo "Skipping raw cleanup (REMOVE_RAW=${REMOVE_RAW:-0}, process exit=$PROCESS_STATUS)"
fi

exit "$PROCESS_STATUS"
