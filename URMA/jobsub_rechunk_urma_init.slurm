#!/bin/bash
#SBATCH --job-name=URMA-rechunk-init
#SBATCH --output=slurmout/URMA-rechunk-init-%j.out
#SBATCH --error=slurmout/URMA-rechunk-init-%j.err
#SBATCH --time=02:00:00
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=2
#SBATCH --mem=8G
#SBATCH --propagate=NONE

# Serially builds the 24h-chunked sibling store's skeleton (one variable at a
# time, in one process) via process_and_write_to_zarr.py --init-only, exactly
# like jobsub_process_and_write_to_zarr_init.slurm does for the canonical
# store -- just pointed at a sibling name via --store-suffix so the real
# URMA_NMS.zarr is never touched. See URMA/run_rechunk_urma.sh and the
# rechunk plan (2026-09) for the full migration this is part of.
#
# Usage: sbatch jobsub_rechunk_urma_init.slurm <region> <store-suffix> <var1> [var2 ...]

mkdir -p slurmout

source /network/rit/lab/basulab/mambaforge/etc/profile.d/conda.sh
conda activate DFSAI

REGION="$1"; shift
STORE_SUFFIX="$1"; shift

if [ -z "$REGION" ] || [ -z "$STORE_SUFFIX" ] || [ "$#" -eq 0 ]; then
    echo "ERROR: Usage: sbatch jobsub_rechunk_urma_init.slurm <region> <store-suffix> <var1> [var2 ...]"
    exit 1
fi

for VAR in "$@"; do
    echo "[rechunk-init] --var_name $VAR --region $REGION --store-suffix $STORE_SUFFIX --time-chunk 24"
    python process_and_write_to_zarr.py --var_name "$VAR" --year 2020 --region "$REGION" \
        --init-only --time-chunk 24 --store-suffix "$STORE_SUFFIX"
done
