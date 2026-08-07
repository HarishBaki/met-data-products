#!/bin/bash
#SBATCH --job-name=URMA-init
#SBATCH --output=slurmout/URMA-init-%j.out
#SBATCH --error=slurmout/URMA-init-%j.err
#SBATCH --time=02:00:00
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=2
#SBATCH --mem=8G

# Serially pre-initializes the output zarr store/variable skeleton for every var
# passed below, one process at a time, inside a single SLURM job -- this is the
# step run_all_process_and_write_to_zarr.sh submits and waits on before fanning
# out the real parallel per-var/year jobs, so multiple jobs never race to create
# the same store (see process_and_write_to_zarr.py --init-only). Low mem/cpu is
# enough: --init-only never reads GRIB data, just writes zarr metadata.
#
# Usage: sbatch jobsub_process_and_write_to_zarr_init.slurm <region> <var1> [var2 ...]

mkdir -p slurmout

source /network/rit/lab/basulab/mambaforge/etc/profile.d/conda.sh
conda activate DFSAI

REGION="$1"; shift

if [ -z "$REGION" ] || [ "$#" -eq 0 ]; then
    echo "ERROR: Usage: sbatch jobsub_process_and_write_to_zarr_init.slurm <region> <var1> [var2 ...]"
    exit 1
fi

for VAR in "$@"; do
    echo "[init] --var_name $VAR --region $REGION"
    python process_and_write_to_zarr.py --var_name "$VAR" --year 2020 --region "$REGION" --init-only
done
