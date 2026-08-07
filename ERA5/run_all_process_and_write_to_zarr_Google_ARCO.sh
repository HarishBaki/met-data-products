#!/bin/bash

set -euo pipefail

# ==========================================================
# CONFIGURATION
# ==========================================================
MAX_PARALLEL=7
JOBSCRIPT="jobsub_process_and_write_to_zarr_Google_ARCO.slurm"
# Set to 1 to print submissions without actually calling sbatch, e.g.:
#   DRY_RUN=1 ./run_all_process_and_write_to_zarr_Google_ARCO.sh
DRY_RUN="${DRY_RUN:-0}"

# Region to process (e.g. New_York, New_Mexico). Export before running, e.g.:
#   REGION=New_Mexico ./run_all_process_and_write_to_zarr_Google_ARCO.sh
REGION="${REGION:-New_York}"
export REGION

# Empty by default -- lets the Python script derive it from REGION. Set to
# override explicitly.
OUTPUT_ZARR="${OUTPUT_ZARR:-}"
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
# PRE-INIT (serial, one job, before any parallel submissions)
# ==========================================================
# Pre-initialize the output zarr group/skeleton for every (group, var) via a
# single serial SLURM job before any parallel jobs run -- otherwise multiple
# jobs targeting the same not-yet-existing store can simultaneously race to
# create it, corrupting it. Especially important here: unlike other
# products, the surface (sl) group writes to the SAME shared zarr store
# across every year in YEAR_START..YEAR_END, so this isn't just a same-year
# race. See process_and_write_to_zarr_Google_ARCO.py --init-only.
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

if [ "${DRY_RUN}" -eq 0 ]; then
  INIT_ENTRIES=()
  if [ "$SUBMIT_SURFACE" -eq 1 ]; then
    for VAR in "${SURFACE_VARS[@]}"; do
      INIT_ENTRIES+=("sl:${VAR}")
    done
  fi
  if [ "$SUBMIT_PRESSURE" -eq 1 ]; then
    for VAR in "${PRESSURE_VARS[@]}"; do
      INIT_ENTRIES+=("pl:${VAR}")
    done
  fi
  if [ "$SUBMIT_MODEL" -eq 1 ]; then
    for VAR in "${MODEL_VARS[@]}"; do
      INIT_ENTRIES+=("ml:${VAR}")
    done
  fi

  init_id=$(sbatch --parsable jobsub_process_and_write_to_zarr_Google_ARCO_init.slurm "$REGION" "${INIT_ENTRIES[@]}")
  echo "Submitted: init job=$init_id"
  echo "Waiting for init job=$init_id to complete..."
  if ! wait_for_jobs "$init_id"; then
    echo "ERROR: init job=$init_id did not complete successfully. Aborting." >&2
    exit 1
  fi
  echo "Pre-init complete -- safe to fan out in parallel now."
fi

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
