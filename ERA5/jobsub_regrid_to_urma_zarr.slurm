#!/bin/bash
#SBATCH --job-name=ERA5-regrid
#SBATCH --output=slurmout/ERA5-regrid-%j.out
#SBATCH --error=slurmout/ERA5-regrid-%j.err
#SBATCH --time=02:00:00
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=32
#SBATCH --mem=80G
#SBATCH --qos=freetier

mkdir -p slurmout

var_name="$1"
START_YEARMONTH="$2"
END_YEARMONTH="$3"
method="$4"
intended_LR_data="$5"

if [ -z "$var_name" ] || [ -z "$START_YEARMONTH" ] || [ -z "$END_YEARMONTH" ] || [ -z "$method" ]; then
    echo "ERROR: Usage: sbatch jobsub_regrid_to_urma_zarr.slurm <var_name> <start_yearmonth> <end_yearmonth> <method> [intended_LR_data]"
    exit 1
fi

source /network/rit/lab/basulab/mambaforge/etc/profile.d/conda.sh
conda activate /network/rit/lab/basulab/conda_envs/hb533188/DFSAI

echo "=========================================================="
echo "Running ERA5 regridding"
echo "Variable: $var_name"
echo "Start: $START_YEARMONTH"
echo "End: $END_YEARMONTH"
echo "Method: $method"
if [ -z "$intended_LR_data" ]; then
    echo "Resolution: HR (URMA 256x288)"
else
    echo "Resolution: LR ($intended_LR_data)"
fi
echo "CPUs requested: $SLURM_CPUS_PER_TASK"
echo "=========================================================="

if [ -z "$intended_LR_data" ]; then
    python regrid_to_urma_zarr.py \
        --var_name "$var_name" \
        --start-yearmonth "$START_YEARMONTH" \
        --end-yearmonth "$END_YEARMONTH" \
        --method "$method"
else
    python regrid_to_urma_zarr.py \
        --var_name "$var_name" \
        --start-yearmonth "$START_YEARMONTH" \
        --end-yearmonth "$END_YEARMONTH" \
        --method "$method" \
        --intended_LR_data "$intended_LR_data"
fi
