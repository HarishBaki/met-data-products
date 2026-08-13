#!/bin/bash
#SBATCH --job-name=nan-times
#SBATCH --output=slurmout/nan-times-%j.out
#SBATCH --error=slurmout/nan-times-%j.err
#SBATCH --time=12:00:00
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=32
#SBATCH --mem=256G
#SBATCH --propagate=NONE
#SBATCH --exclude=dgx17,dgx18

# Scans a product's full zarr store history for NaN timestamps -- see
# compute_nan_times.py. --propagate=NONE avoids inheriting a restrictive
# submitting-shell ulimit (bit us in production for HRRR/ERA5-ARCO/ICON
# earlier this session -- see git history). --exclude=dgx17,dgx18: both
# nodes have a confirmed infrastructure problem (OOM/instability on jobs
# that fit comfortably elsewhere) diagnosed earlier this session.
#
# Usage: sbatch jobsub_compute_nan_times.slurm <product> <region>
#   e.g.: sbatch jobsub_compute_nan_times.slurm ICON-DREAM-Global New_Mexico

mkdir -p slurmout

PRODUCT="$1"
REGION="$2"

if [ -z "$PRODUCT" ] || [ -z "$REGION" ]; then
    echo "ERROR: Usage: sbatch jobsub_compute_nan_times.slurm <product> <region>" >&2
    exit 1
fi

source /network/rit/lab/basulab/mambaforge/etc/profile.d/conda.sh
conda activate /network/rit/lab/basulab/conda_envs/hb533188/DFSAI

echo "=========================================================="
echo "Computing NaN times"
echo "Product: $PRODUCT"
echo "Region: $REGION"
echo "=========================================================="

python compute_nan_times.py --product "$PRODUCT" --region "$REGION"
