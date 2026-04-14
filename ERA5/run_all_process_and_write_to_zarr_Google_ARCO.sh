#!/bin/bash

set -euo pipefail

# ==========================================================
# CONFIGURATION
# ==========================================================
MAX_PARALLEL=1
JOBSCRIPT="jobsub_process_and_write_to_zarr_Google_ARCO.slurm"
DRY_RUN=0

OUTPUT_ZARR="/network/rit/lab/basulab/Projects/DFS/DATA/ERA5_NYS/ERA5_analysis_ARCO_NYS.zarr"
FULL_START_YEAR=1940
FULL_END_YEAR=2050
SURFACE_N_JOBS=32
PRESSURE_N_JOBS=4
MODEL_N_JOBS=4

# Year-wise submission range (inclusive)
YEAR_START=2018
YEAR_END=2025

# Surface variables (group=sl)
SURFACE_VARS=(
  u10
  v10
  t2m
  d2m
  sp
  tp
  i10fg
  si10
  wdir10
  msl
  blh
  cape
  cin
  tcc
)

# Pressure-level variables. Pass "all" because ARCO pressure chunks include all 37 levels per hour.
PRESSURE_VARS=(u v t q z)
PRESSURE_LEVELS="all"

# Model-level variables + selected levels (comma-separated)
MODEL_VARS=(u v t q)
MODEL_LEVELS="137"

# Toggle categories
SUBMIT_SURFACE=1
SUBMIT_PRESSURE=0
SUBMIT_MODEL=0

# ==========================================================
# HELPERS
# ==========================================================
wait_for_slot() {
  while [ "$(squeue -u "$USER" -h -n ERA5_ARCO | wc -l)" -ge "$MAX_PARALLEL" ]; do
    echo "Reached MAX_PARALLEL=${MAX_PARALLEL} ERA5_ARCO jobs. Waiting..."
    sleep 30
  done
}

submit_job() {
  local var_name="$1"
  local group="$2"
  local pressure_levels="$3"
  local model_levels="$4"
  local process_start="$5"
  local process_end="$6"
  local n_jobs="$7"
  local source_var="${8:-none}"
  local target_var="${9:-none}"

  wait_for_slot
  local CMD=(
    sbatch "${JOBSCRIPT}"
    "${var_name}"
    "${group}"
    "${pressure_levels}"
    "${model_levels}"
    "${process_start}"
    "${process_end}"
    "${source_var}"
    "${target_var}"
    "${OUTPUT_ZARR}"
    "${FULL_START_YEAR}"
    "${FULL_END_YEAR}"
    "${n_jobs}"
  )

  echo "Submitting ${group} var=${var_name} p=${pressure_levels} m=${model_levels} ${process_start} -> ${process_end}"
  printf 'CMD:'
  printf ' %q' "${CMD[@]}"
  printf '\n'

  if [ "${DRY_RUN}" -eq 0 ]; then
    "${CMD[@]}"
  fi
}

# ==========================================================
# MAIN
# ==========================================================
for YEAR in $(seq "$YEAR_START" "$YEAR_END"); do
  PROCESS_START="${YEAR}-01-01"
  PROCESS_END="${YEAR}-12-31T23:00:00"

  if [ "$SUBMIT_SURFACE" -eq 1 ]; then
    for VAR in "${SURFACE_VARS[@]}"; do
      submit_job "${VAR}" "sl" "none" "none" "${PROCESS_START}" "${PROCESS_END}" "${SURFACE_N_JOBS}" "none" "none"
      sleep 1
    done
  fi

  if [ "$SUBMIT_PRESSURE" -eq 1 ]; then
    for VAR in "${PRESSURE_VARS[@]}"; do
      submit_job "${VAR}" "pl" "${PRESSURE_LEVELS}" "none" "${PROCESS_START}" "${PROCESS_END}" "${PRESSURE_N_JOBS}" "none" "none"
      sleep 1
    done
  fi

  if [ "$SUBMIT_MODEL" -eq 1 ]; then
    for VAR in "${MODEL_VARS[@]}"; do
      submit_job "${VAR}" "ml" "none" "${MODEL_LEVELS}" "${PROCESS_START}" "${PROCESS_END}" "${MODEL_N_JOBS}" "none" "none"
      sleep 1
    done
  fi
done

echo "=============================================="
echo "All ERA5 ARCO jobs submitted."
echo "=============================================="
