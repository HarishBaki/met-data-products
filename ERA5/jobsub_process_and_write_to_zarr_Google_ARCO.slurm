#!/bin/bash
#SBATCH --job-name=ERA5_ARCO
#SBATCH --output=slurmout/ERA5_ARCO-%j.out
#SBATCH --error=slurmout/ERA5_ARCO-%j.err
#SBATCH --time=06:00:00
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=32
#SBATCH --mem=250G

set -euo pipefail

mkdir -p slurmout

############################################################
# READ ARGUMENTS PASSED TO SBATCH
#
# Usage:
# sbatch jobsub_process_and_write_to_zarr_Google_ARCO.slurm \
#   <var_name> <group> <pressure_levels> <model_levels> <process_start> <process_end> \
#   [source_var] [target_var] [output_zarr] [full_start_year] [full_end_year] [n_jobs]
#
# group: sl | pl | ml | auto
# pressure_levels/model_levels:
#   - pass \"none\" when not used
#   - pass \"all\" for all pressure levels
#   - pass single value (e.g. 850)
#   - or comma list (e.g. 137,136,135 for model levels)
############################################################
VAR_NAME="${1:-z}"
GROUP="${2:-pl}"
PRESSURE_LEVELS="${3:-all}"
MODEL_LEVELS="${4:-none}"
PROCESS_START="${5:-2015-01-01T00}"
PROCESS_END="${6:-2015-01-31T23}"
SOURCE_VAR="${7:-none}"
TARGET_VAR="${8:-none}"
OUTPUT_ZARR="${9:-/network/rit/lab/basulab/Projects/DFS/DATA/ERA5_NYS/ERA5_analysis_ARCO_NYS.zarr}"
FULL_START_YEAR="${10:-1940}"
FULL_END_YEAR="${11:-2050}"
N_JOBS_ARG="${12:-31}"

if [ -z "${VAR_NAME}" ] || [ -z "${PROCESS_START}" ] || [ -z "${PROCESS_END}" ]; then
  echo "ERROR: Missing required args."
  echo "Usage:"
  echo "  sbatch jobsub_process_and_write_to_zarr_Google_ARCO.slurm \\"
  echo "    <var_name> <group> <pressure_levels> <model_levels> <process_start> <process_end> \\"
  echo "    [source_var] [target_var] [output_zarr] [full_start_year] [full_end_year] [n_jobs]"
  exit 1
fi

############################################################
# ACTIVATE ENVIRONMENT
############################################################
source /network/rit/lab/basulab/mambaforge/etc/profile.d/conda.sh
# Some activate hooks assume MAGPLUS_HOME exists; define empty default to satisfy `set -u`.
export MAGPLUS_HOME="${MAGPLUS_HOME:-}"
conda activate /network/rit/lab/basulab/conda_envs/hb533188/DFSAI

N_JOBS="${N_JOBS_ARG:-${SLURM_CPUS_PER_TASK:-1}}"

echo "=========================================================="
echo "Running ERA5 Google ARCO processing"
echo "var_name       : ${VAR_NAME}"
echo "group          : ${GROUP}"
echo "pressure_levels: ${PRESSURE_LEVELS}"
echo "model_levels   : ${MODEL_LEVELS}"
echo "process_start  : ${PROCESS_START}"
echo "process_end    : ${PROCESS_END}"
echo "source_var     : ${SOURCE_VAR}"
echo "target_var     : ${TARGET_VAR}"
echo "output_zarr    : ${OUTPUT_ZARR}"
echo "full_start_year: ${FULL_START_YEAR}"
echo "full_end_year  : ${FULL_END_YEAR}"
echo "n_jobs         : ${N_JOBS}"
echo "=========================================================="

CMD=(
  python "process_and_write_to_zarr_Google_ARCO.py"
  --var-name "${VAR_NAME}"
  --group "${GROUP}"
  --process-start "${PROCESS_START}"
  --process-end "${PROCESS_END}"
  --output-zarr "${OUTPUT_ZARR}"
  --full-start-year "${FULL_START_YEAR}"
  --full-end-year "${FULL_END_YEAR}"
  --n-jobs "${N_JOBS}"
  --consolidate-metadata
)

if [ "${PRESSURE_LEVELS}" != "none" ]; then
  CMD+=(--pressure-levels "${PRESSURE_LEVELS}")
fi
if [ "${MODEL_LEVELS}" != "none" ]; then
  CMD+=(--model-levels "${MODEL_LEVELS}")
fi
if [ "${SOURCE_VAR}" != "none" ]; then
  CMD+=(--source-var "${SOURCE_VAR}")
fi
if [ "${TARGET_VAR}" != "none" ]; then
  CMD+=(--target-var "${TARGET_VAR}")
fi

"${CMD[@]}"
