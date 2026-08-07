#!/bin/bash

set -euo pipefail

JOBSCRIPT="jobsub_process_and_write_to_zarr_HRRRzarr.slurm"
MAX_PARALLEL=7  # freetier QOS allows 8 jobs/user; stay one below the limit
PROCESS_START="2018-01-01T00"
PROCESS_END="2025-12-31T23"

# Region to process (e.g. New_York, New_Mexico). Export before running, e.g.:
#   REGION=New_Mexico ./run_all_process_and_write_to_zarr_HRRRzarr.sh
REGION="${REGION:-New_York}"
export REGION

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

# Pre-initialize the output zarr store/skeleton for every var via a single
# serial SLURM job before any parallel jobs run -- otherwise multiple jobs
# targeting the same not-yet-existing store can simultaneously race to create
# it, corrupting it (zarr.errors.GroupNotFoundError / "Time coordinate
# mismatch" / stale-handle failures -- observed in production from this exact
# race). See process_and_write_to_zarr_HRRRzarr.py --init-only. Source vars
# listed before derived vars: the derived pipeline requires the store to
# already exist.
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

INIT_ENTRIES=()
for VAR in "${VARS[@]}"; do
  INIT_ENTRIES+=("source:${VAR}")
done
for VAR in "${DERIVED_VARS[@]}"; do
  INIT_ENTRIES+=("derived:${VAR}")
done
init_id=$(sbatch --parsable jobsub_process_and_write_to_zarr_HRRRzarr_init.slurm "$REGION" "$PROCESS_START" "$PROCESS_END" "${INIT_ENTRIES[@]}")
echo "Submitted: init job=$init_id"
echo "Waiting for init job=$init_id to complete..."
if ! wait_for_jobs "$init_id"; then
  echo "ERROR: init job=$init_id did not complete successfully. Aborting." >&2
  exit 1
fi
echo "Pre-init complete -- safe to fan out in parallel now."

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
