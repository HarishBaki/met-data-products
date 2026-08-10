#!/bin/bash
#SBATCH --job-name=ICON-DREAM-init
#SBATCH --output=slurmout/ICON-DREAM-init-%j.out
#SBATCH --error=slurmout/ICON-DREAM-init-%j.err
#SBATCH --time=02:00:00
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=2
#SBATCH --mem=8G
#SBATCH --propagate=NONE
#SBATCH --exclude=dgx17,dgx18

# Serially pre-initializes the output zarr store/variable skeleton for every
# var passed below, one process at a time, inside a single SLURM job -- this
# is the step run_all_process_and_write_to_zarr.sh submits and waits on
# before fanning out the real parallel jobs, so multiple jobs never race to
# create the same store (see process_and_write_to_zarr.py --init-only).
#
# Usage: sbatch jobsub_process_and_write_to_zarr_init.slurm <region> <var1> [var2 ...]

mkdir -p slurmout

source /network/rit/lab/basulab/mambaforge/etc/profile.d/conda.sh
conda activate /network/rit/lab/basulab/conda_envs/hb533188/DFSAI

REGION="$1"; shift

if [ -z "$REGION" ] || [ "$#" -eq 0 ]; then
    echo "ERROR: Usage: sbatch jobsub_process_and_write_to_zarr_init.slurm <region> <var1> [var2 ...]"
    exit 1
fi

for VAR in "$@"; do
    echo "[init] --var_name $VAR --region $REGION"
    python process_and_write_to_zarr.py \
        --var_name "$VAR" \
        --start-yearmonth 202001 \
        --end-yearmonth 202001 \
        --region "$REGION" \
        --init-only
done
