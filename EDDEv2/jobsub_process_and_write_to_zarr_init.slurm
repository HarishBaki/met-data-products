#!/bin/bash
#SBATCH --job-name=EDDEv2-init
#SBATCH --output=slurmout/EDDEv2-init-%j.out
#SBATCH --error=slurmout/EDDEv2-init-%j.err
#SBATCH --time=02:00:00
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=2
#SBATCH --mem=8G

# Serially pre-initializes the output zarr store/variable skeleton for every
# (run_type, var) pair passed below, one process at a time, inside a single
# SLURM job -- this is the step run_all_process_and_write_to_zarr.sh submits
# and waits on before fanning out the real parallel jobs, so multiple jobs
# never race to create the same store (see process_and_write_to_zarr.py
# --init-only). Each run_type writes to its own separate zarr store, so the
# race only matters within the same run_type, but all entries are run here
# regardless for simplicity.
#
# Entries are "run_type:var", e.g. "SSP2-4.5:si10".
#
# Usage: sbatch jobsub_process_and_write_to_zarr_init.slurm <region> run_type1:var1 [run_type2:var2 ...]

mkdir -p slurmout

source /network/rit/lab/basulab/mambaforge/etc/profile.d/conda.sh
conda activate DFSAI

REGION="$1"; shift

if [ -z "$REGION" ] || [ "$#" -eq 0 ]; then
    echo "ERROR: Usage: sbatch jobsub_process_and_write_to_zarr_init.slurm <region> run_type1:var1 [...]"
    exit 1
fi

for ENTRY in "$@"; do
    RUN_TYPE="${ENTRY%:*}"
    VAR_NAME="${ENTRY##*:}"
    echo "[init] run_type=${RUN_TYPE} var_name=${VAR_NAME} region=${REGION}"
    python process_and_write_to_zarr.py \
        --run-type "${RUN_TYPE}" \
        --var-name "${VAR_NAME}" \
        --process-start "2020-01" \
        --process-end "2020-01" \
        --region "${REGION}" \
        --init-only
done
