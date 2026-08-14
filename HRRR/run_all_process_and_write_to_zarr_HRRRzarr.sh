#!/bin/bash

set -euo pipefail

JOBSCRIPT="jobsub_process_and_write_to_zarr_HRRRzarr.slurm"
# freetier QOS cap; unfiltered count below absorbs any non-freetier jobs (e.g.
# vscode-dgx) automatically. Override without editing the file, e.g.:
#   MAX_PARALLEL=4 REGION=New_Mexico ./run_all_process_and_write_to_zarr_HRRRzarr.sh
MAX_PARALLEL="${MAX_PARALLEL:-8}"
PROCESS_START="2018-01-01T00"
PROCESS_END="2025-12-31T23"

# Region to process (e.g. New_York, New_Mexico). Export before running, e.g.:
#   REGION=New_Mexico ./run_all_process_and_write_to_zarr_HRRRzarr.sh
REGION="${REGION:?REGION must be set (e.g. REGION=New_Mexico) -- no default, to avoid silently running against the wrong region}"
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
  # Deliberately not QOS-filtered: an unfiltered count is always >= any single QOS's
  # count, so it can never let a submission through that SLURM would reject, and it
  # automatically absorbs non-freetier jobs (e.g. vscode-dgx) without needing
  # MAX_PARALLEL manually tuned down to leave room.
  while [ "$(squeue -u "$USER" -h | wc -l)" -ge "$MAX_PARALLEL" ]; do
    echo "Reached MAX_PARALLEL=${MAX_PARALLEL} jobs. Waiting..."
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
# Same throttle wait_for_slot() applies below -- without it here, the init submission
# assumes a free slot always exists, which only holds if nothing else is using the
# shared quota. In a sequential product chain, the previous product's own jobs can
# still be running right when this fires, causing QOSMaxSubmitJobPerUserLimit on the
# init call itself.
wait_for_slot
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

# Consolidate once, after every job above has finished. Blocking poll rather than
# --dependency=afterany: SLURM purges completed jobs from squeue/sacct after
# MinJobAge (300s on this cluster), and a long submission loop easily exceeds that
# gap, so a dependency string built from early job IDs would reference already-purged
# jobs and fail at submission time (same reasoning as Ouranos's run_all script).
# Not afterok-equivalent either: a single stray var/month failure shouldn't block
# consolidating whatever did succeed, so failures here are only logged, not fatal --
# see consolidate_metadata.py. Previously each job passed --consolidate-metadata
# itself (removed from jobsub_*.slurm) -- that repeated the same full-store scan
# once per job instead of once total.
all_ids=("${JOB_IDS[@]}")
if [ "${#all_ids[@]}" -gt 0 ]; then
  echo "Waiting for all ${#all_ids[@]} jobs to finish before consolidating..."
  wait_for_jobs "${all_ids[@]}" || echo "WARNING: not every job COMPLETED -- consolidating anyway."
  mkdir -p slurmout
  finalize_id=$(sbatch --parsable \
    --job-name=consolidate-HRRR \
    --output=slurmout/consolidate-HRRR-%j.out --error=slurmout/consolidate-HRRR-%j.err \
    --time=02:00:00 --cpus-per-task=2 --mem=32G --propagate=NONE \
    --wrap="source /network/rit/lab/basulab/mambaforge/etc/profile.d/conda.sh && conda activate /network/rit/lab/basulab/conda_envs/hb533188/DFSAI && cd .. && python consolidate_metadata.py --product HRRR --region $REGION")
  echo "Submitted: consolidate job=$finalize_id (depends on all ${#all_ids[@]} jobs above)"
fi
