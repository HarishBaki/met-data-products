#!/bin/bash
#SBATCH --job-name=HRRR_ZARR
#SBATCH --output=slurmout/HRRR_ZARR-%j.out
#SBATCH --error=slurmout/HRRR_ZARR-%j.err
#SBATCH --time=06:00:00
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=32
#SBATCH --mem=180G

set -euo pipefail
umask 0002

mkdir -p slurmout

############################################################
# READ ARGUMENTS PASSED TO SBATCH
#
# Usage:
# sbatch jobsub_process_and_write_to_zarr_HRRRzarr.slurm \
#   <var_name> <process_start> <process_end> \
#   [output_zarr] [full_start_year] [full_end_year] [orog_path] [time_chunk] [y_chunk] [x_chunk]
############################################################
VAR_NAME="${1:-i10fg}"
PROCESS_START="${2:-2023-01-01T00}"
PROCESS_END="${3:-2023-12-31T23}"
OUTPUT_ZARR="${4:-/network/rit/lab/basulab/Projects/DFS/DATA/HRRR_NYS/HRRR_NYS.zarr}"
FULL_START_YEAR="${5:-2010}"
FULL_END_YEAR="${6:-2040}"
OROG_PATH="${7:-/network/rit/lab/basulab/Projects/DFS/DATA/HRRR_NYS/hrrr_orography_cropped_nys.nc}"
TIME_CHUNK="${8:-24}"
Y_CHUNK="${9:-128}"
X_CHUNK="${10:-144}"

if [ -z "${VAR_NAME}" ] || [ -z "${PROCESS_START}" ] || [ -z "${PROCESS_END}" ]; then
  echo "ERROR: Missing required args."
  echo "Usage:"
  echo "  sbatch jobsub_process_and_write_to_zarr_HRRRzarr.slurm \\"
  echo "    <var_name> <process_start> <process_end> \\"
  echo "    [output_zarr] [full_start_year] [full_end_year] [orog_path] [time_chunk] [y_chunk] [x_chunk]"
  exit 1
fi

source /network/rit/lab/basulab/mambaforge/etc/profile.d/conda.sh
export MAGPLUS_HOME="${MAGPLUS_HOME:-}"
export PYTHONUNBUFFERED=1
conda activate /network/rit/lab/basulab/conda_envs/hb533188/DFSAI

N_JOBS="${SLURM_CPUS_PER_TASK:-1}"

OUTPUT_PARENT="$(dirname "${OUTPUT_ZARR}")"
if [ ! -d "${OUTPUT_PARENT}" ]; then
  mkdir -p "${OUTPUT_PARENT}"
fi
chmod g+rws "${OUTPUT_PARENT}" || true
chgrp basulab "${OUTPUT_PARENT}" || true

echo "=========================================================="
echo "Running HRRR Utah Zarr processing"
echo "var_name       : ${VAR_NAME}"
echo "process_start  : ${PROCESS_START}"
echo "process_end    : ${PROCESS_END}"
echo "output_zarr    : ${OUTPUT_ZARR}"
echo "full_start_year: ${FULL_START_YEAR}"
echo "full_end_year  : ${FULL_END_YEAR}"
echo "orog_path      : ${OROG_PATH}"
echo "time_chunk     : ${TIME_CHUNK}"
echo "y_chunk        : ${Y_CHUNK:-<default>}"
echo "x_chunk        : ${X_CHUNK:-<default>}"
echo "n_jobs         : ${N_JOBS}"
echo "=========================================================="

cmd=(
  python "process_and_write_to_zarr_HRRRzarr.py"
  --var-name "${VAR_NAME}"
  --process-start "${PROCESS_START}"
  --process-end "${PROCESS_END}"
  --output-zarr "${OUTPUT_ZARR}"
  --full-start-year "${FULL_START_YEAR}"
  --full-end-year "${FULL_END_YEAR}"
  --orog-path "${OROG_PATH}"
  --time-chunk "${TIME_CHUNK}"
  --y-chunk "${Y_CHUNK}"
  --x-chunk "${X_CHUNK}"
  --n-jobs "${N_JOBS}"
  --skip-complete-months
  --consolidate-metadata
)

"${cmd[@]}"
