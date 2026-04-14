#!/bin/bash

set -euo pipefail

JOBSCRIPT="jobsub_process_and_write_to_zarr_HRRRzarr.slurm"
MAX_PARALLEL=7

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

wait_for_slot() {
  while [ "$(squeue -u "$USER" -h -n HRRR_ZARR | wc -l)" -ge "$MAX_PARALLEL" ]; do
    echo "Reached MAX_PARALLEL=${MAX_PARALLEL} HRRR_ZARR jobs. Waiting..."
    sleep 30
  done
}

for VAR in "${VARS[@]}"; do
  wait_for_slot
  echo "Submitting HRRR job for var_name=${VAR}"
  sbatch "${JOBSCRIPT}" "${VAR}"
  sleep 1
done

echo "=============================================="
echo "All HRRR jobs submitted."
echo "=============================================="
