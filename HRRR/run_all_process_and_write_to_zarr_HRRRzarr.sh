#!/bin/bash

set -euo pipefail

JOBSCRIPT="jobsub_process_and_write_to_zarr_HRRRzarr.slurm"
MAX_PARALLEL=7  # freetier QOS allows 8 jobs/user; stay one below the limit
PROCESS_START="2018-01-01T00"
PROCESS_END="2025-12-31T23"

# Hard-define the variables you want to submit here.
VARS=(
  u10
  v10
  t2m
  d2m
  sh2
  sp
  tp
  i10fg
)
DERIVED_VARS=(
  si10
  wdir10
)

declare -A JOB_IDS

wait_for_slot() {
  while [ "$(squeue -u "$USER" --qos=freetier -h | wc -l)" -ge "$MAX_PARALLEL" ]; do
    echo "Reached MAX_PARALLEL=${MAX_PARALLEL} jobs under QOS freetier. Waiting..."
    sleep 30
  done
}

submit_job() {
  local mode="$1"
  local var_name="$2"
  local dependency="${3:-}"
  local -a cmd=(sbatch --parsable)

  if [ -n "$dependency" ]; then
    cmd+=(--dependency "$dependency")
  fi

  cmd+=("$JOBSCRIPT" "$mode" "$var_name" "$PROCESS_START" "$PROCESS_END")
  "${cmd[@]}"
}

for VAR in "${VARS[@]}"; do
  wait_for_slot
  echo "Submitting HRRR source job for var_name=${VAR}"
  JOB_IDS["$VAR"]="$(submit_job source "${VAR}")"
  echo "Submitted job ${JOB_IDS[$VAR]} for ${VAR}"
  sleep 1
done

DERIVED_DEPENDENCY=""
if [ -n "${JOB_IDS[u10]:-}" ] && [ -n "${JOB_IDS[v10]:-}" ]; then
  DERIVED_DEPENDENCY="afterok:${JOB_IDS[u10]}:${JOB_IDS[v10]}"
fi

for VAR in "${DERIVED_VARS[@]}"; do
  wait_for_slot
  echo "Submitting HRRR derived job for var_name=${VAR}"
  JOB_IDS["$VAR"]="$(submit_job derived "${VAR}" "${DERIVED_DEPENDENCY}")"
  echo "Submitted job ${JOB_IDS[$VAR]} for ${VAR}"
  sleep 1
done

echo "=============================================="
echo "All HRRR jobs submitted."
echo "=============================================="
