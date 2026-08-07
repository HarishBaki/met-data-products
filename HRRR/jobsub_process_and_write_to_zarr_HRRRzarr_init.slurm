#!/bin/bash
#SBATCH --job-name=HRRR-init
#SBATCH --output=slurmout/HRRR-init-%j.out
#SBATCH --error=slurmout/HRRR-init-%j.err
#SBATCH --time=02:00:00
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=2
#SBATCH --mem=8G

# Serially pre-initializes the output zarr store/variable skeleton for every
# var passed below, one process at a time, inside a single SLURM job -- this
# is the step run_all_process_and_write_to_zarr_HRRRzarr.sh submits and waits
# on before fanning out the real parallel per-var jobs, so multiple jobs never
# race to create the same store (see process_and_write_to_zarr_HRRRzarr.py
# --init-only). Low mem/cpu is enough: --init-only never opens source data.
#
# Entries are "source:<var>" or "derived:<var>" -- pass source vars before
# derived vars so the store already exists (created by the first source
# entry) by the time a derived entry's --init-only runs, since the derived
# pipeline requires the store to already exist.
#
# Usage: sbatch jobsub_process_and_write_to_zarr_HRRRzarr_init.slurm <region> <process_start> <process_end> source:<var1> [source:<var2> ...] [derived:<var> ...]

set -euo pipefail

mkdir -p slurmout

source /network/rit/lab/basulab/mambaforge/etc/profile.d/conda.sh
export MAGPLUS_HOME="${MAGPLUS_HOME:-}"
export PYTHONUNBUFFERED=1
conda activate /network/rit/lab/basulab/conda_envs/hb533188/DFSAI

REGION="$1"; PROCESS_START="$2"; PROCESS_END="$3"; shift 3

if [ -z "${REGION}" ] || [ "$#" -eq 0 ]; then
    echo "ERROR: Usage: sbatch jobsub_process_and_write_to_zarr_HRRRzarr_init.slurm <region> <process_start> <process_end> source:<var1> [...] [derived:<var> ...]"
    exit 1
fi

for ENTRY in "$@"; do
    MODE="${ENTRY%%:*}"
    VAR_NAME="${ENTRY#*:}"
    echo "[init] mode=${MODE} var_name=${VAR_NAME} region=${REGION}"
    python process_and_write_to_zarr_HRRRzarr.py "${MODE}" \
        --var-name "${VAR_NAME}" \
        --process-start "${PROCESS_START}" \
        --process-end "${PROCESS_END}" \
        --region "${REGION}" \
        --init-only
done
