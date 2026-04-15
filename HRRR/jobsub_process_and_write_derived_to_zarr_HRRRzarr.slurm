#!/bin/bash
#SBATCH --job-name=HRRR_ZARR
#SBATCH --output=slurmout/HRRR_ZARR_DERIVED-%j.out
#SBATCH --error=slurmout/HRRR_ZARR_DERIVED-%j.err
#SBATCH --time=06:00:00
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=8
#SBATCH --mem=64G

set -euo pipefail
umask 0002

mkdir -p slurmout

VAR_NAME="${1:-si10}"
PROCESS_START="${2:-2023-01-01T00}"
PROCESS_END="${3:-2023-12-31T23}"
OUTPUT_ZARR="${4:-/network/rit/lab/basulab/Projects/DFS/DATA/HRRR_NYS/HRRR_NYS.zarr}"
OROG_PATH="${5:-/network/rit/lab/basulab/Projects/DFS/DATA/HRRR_NYS/hrrr_orography_cropped_nys.nc}"
TIME_CHUNK="${6:-24}"
Y_CHUNK="${7:-}"
X_CHUNK="${8:-}"

if [ -z "${VAR_NAME}" ] || [ -z "${PROCESS_START}" ] || [ -z "${PROCESS_END}" ]; then
  echo "ERROR: Missing required args."
  echo "Usage:"
  echo "  sbatch jobsub_process_and_write_derived_to_zarr_HRRRzarr.slurm \\"
  echo "    <var_name> <process_start> <process_end> \\"
  echo "    [output_zarr] [orog_path] [time_chunk] [y_chunk] [x_chunk]"
  exit 1
fi

source /network/rit/lab/basulab/mambaforge/etc/profile.d/conda.sh
export MAGPLUS_HOME="${MAGPLUS_HOME:-}"
export PYTHONUNBUFFERED=1
conda activate /network/rit/lab/basulab/conda_envs/hb533188/DFSAI

OUTPUT_PARENT="$(dirname "${OUTPUT_ZARR}")"
if [ ! -d "${OUTPUT_PARENT}" ]; then
  mkdir -p "${OUTPUT_PARENT}"
fi
chmod g+rws "${OUTPUT_PARENT}" || true
chgrp basulab "${OUTPUT_PARENT}" || true

echo "=========================================================="
echo "Running HRRR derived Zarr processing"
echo "var_name      : ${VAR_NAME}"
echo "process_start : ${PROCESS_START}"
echo "process_end   : ${PROCESS_END}"
echo "output_zarr   : ${OUTPUT_ZARR}"
echo "orog_path     : ${OROG_PATH}"
echo "time_chunk    : ${TIME_CHUNK}"
echo "y_chunk       : ${Y_CHUNK:-<default>}"
echo "x_chunk       : ${X_CHUNK:-<default>}"
echo "=========================================================="

cmd=(
  python "process_and_write_derived_to_zarr_HRRRzarr.py"
  --var-name "${VAR_NAME}"
  --process-start "${PROCESS_START}"
  --process-end "${PROCESS_END}"
  --output-zarr "${OUTPUT_ZARR}"
  --orog-path "${OROG_PATH}"
  --time-chunk "${TIME_CHUNK}"
  --skip-complete-months
  --consolidate-metadata
)

if [ -n "${Y_CHUNK}" ]; then
  cmd+=(--y-chunk "${Y_CHUNK}")
fi

if [ -n "${X_CHUNK}" ]; then
  cmd+=(--x-chunk "${X_CHUNK}")
fi

"${cmd[@]}"
