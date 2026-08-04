#!/bin/bash
#SBATCH --job-name=HRRR_ZARR
#SBATCH --output=slurmout/HRRR_ZARR-%j.out
#SBATCH --error=slurmout/HRRR_ZARR-%j.err
#SBATCH --time=06:00:00
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=32
#SBATCH --mem=80G
#SBATCH --qos=freetier

set -euo pipefail
umask 0002

mkdir -p slurmout

############################################################
# READ ARGUMENTS PASSED TO SBATCH
#
# Usage:
# sbatch jobsub_process_and_write_to_zarr_HRRRzarr.slurm \
#   [source|derived] <var_name> <process_start> <process_end> \
#   [output_zarr] [full_start_year] [full_end_year] [orog_path] [time_chunk] [y_chunk] [x_chunk]
#
# Backward compatible source usage is still supported:
# sbatch jobsub_process_and_write_to_zarr_HRRRzarr.slurm \
#   <var_name> <process_start> <process_end>
#
# [output_zarr]/[orog_path] default to empty, letting the Python script derive
# them from REGION (env var, default New_York -- configs/regions/{region}.yaml)
# -- pass them explicitly only to override that, e.g.:
#   REGION=New_Mexico sbatch jobsub_process_and_write_to_zarr_HRRRzarr.slurm source t2m 2025-01-01T00 2025-12-31T23
############################################################
MODE="source"
if [ "${1:-}" = "source" ] || [ "${1:-}" = "derived" ]; then
  MODE="$1"
  shift
fi

if [ "${MODE}" = "derived" ]; then
  DEFAULT_VAR_NAME="si10"
  DEFAULT_Y_CHUNK=""
  DEFAULT_X_CHUNK=""
else
  DEFAULT_VAR_NAME="i10fg"
  DEFAULT_Y_CHUNK="128"
  DEFAULT_X_CHUNK="144"
fi

VAR_NAME="${1:-${DEFAULT_VAR_NAME}}"
PROCESS_START="${2:-2023-01-01T00}"
PROCESS_END="${3:-2023-12-31T23}"
OUTPUT_ZARR="${4:-}"
FULL_START_YEAR="${5:-2010}"
FULL_END_YEAR="${6:-2040}"
OROG_PATH="${7:-}"
TIME_CHUNK="${8:-24}"
Y_CHUNK="${9:-${DEFAULT_Y_CHUNK}}"
X_CHUNK="${10:-${DEFAULT_X_CHUNK}}"
REGION="${REGION:-New_York}"

if [ -z "${VAR_NAME}" ] || [ -z "${PROCESS_START}" ] || [ -z "${PROCESS_END}" ]; then
  echo "ERROR: Missing required args."
  echo "Usage:"
  echo "  sbatch jobsub_process_and_write_to_zarr_HRRRzarr.slurm \\"
  echo "    [source|derived] <var_name> <process_start> <process_end> \\"
  echo "    [output_zarr] [full_start_year] [full_end_year] [orog_path] [time_chunk] [y_chunk] [x_chunk]"
  exit 1
fi

source /network/rit/lab/basulab/mambaforge/etc/profile.d/conda.sh
export MAGPLUS_HOME="${MAGPLUS_HOME:-}"
export PYTHONUNBUFFERED=1
conda activate /network/rit/lab/basulab/conda_envs/hb533188/DFSAI

N_JOBS="${SLURM_CPUS_PER_TASK:-1}"

# Only touch/prep a parent dir when OUTPUT_ZARR is explicitly given -- when
# left empty, the Python script derives the path from REGION itself, and for
# an existing region (New_York) that directory (and its permissions) already
# exists; for a brand-new region, whoever sets its data_root should also
# create/chmod its DATA root once, same as prior regions did.
if [ -n "${OUTPUT_ZARR}" ]; then
  OUTPUT_PARENT="$(dirname "${OUTPUT_ZARR}")"
  if [ ! -d "${OUTPUT_PARENT}" ]; then
    mkdir -p "${OUTPUT_PARENT}"
  fi
  chmod g+rws "${OUTPUT_PARENT}" || true
  chgrp basulab "${OUTPUT_PARENT}" || true
fi

echo "=========================================================="
echo "Running HRRR Utah Zarr processing"
echo "mode           : ${MODE}"
echo "var_name       : ${VAR_NAME}"
echo "process_start  : ${PROCESS_START}"
echo "process_end    : ${PROCESS_END}"
echo "output_zarr    : ${OUTPUT_ZARR:-<region-derived>}"
echo "full_start_year: ${FULL_START_YEAR}"
echo "full_end_year  : ${FULL_END_YEAR}"
echo "orog_path      : ${OROG_PATH:-<region-derived>}"
echo "time_chunk     : ${TIME_CHUNK}"
echo "y_chunk        : ${Y_CHUNK:-<default>}"
echo "x_chunk        : ${X_CHUNK:-<default>}"
echo "n_jobs         : ${N_JOBS}"
echo "region         : ${REGION}"
echo "=========================================================="

cmd=(
  python "process_and_write_to_zarr_HRRRzarr.py"
  "${MODE}"
  --var-name "${VAR_NAME}"
  --process-start "${PROCESS_START}"
  --process-end "${PROCESS_END}"
  --full-start-year "${FULL_START_YEAR}"
  --full-end-year "${FULL_END_YEAR}"
  --time-chunk "${TIME_CHUNK}"
  --n-jobs "${N_JOBS}"
  --region "${REGION}"
  --skip-complete-months
  --consolidate-metadata
)

if [ -n "${OUTPUT_ZARR}" ]; then
  cmd+=(--output-zarr "${OUTPUT_ZARR}")
fi

if [ -n "${OROG_PATH}" ]; then
  cmd+=(--orog-path "${OROG_PATH}")
fi

if [ -n "${Y_CHUNK}" ]; then
  cmd+=(--y-chunk "${Y_CHUNK}")
fi

if [ -n "${X_CHUNK}" ]; then
  cmd+=(--x-chunk "${X_CHUNK}")
fi

"${cmd[@]}"
