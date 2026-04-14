#!/bin/bash
#SBATCH --job-name=ICON-DREAM
#SBATCH --output=slurmout/ICON-DREAM-%j.out
#SBATCH --error=slurmout/ICON-DREAM-%j.err
#SBATCH --time=02:00:00
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=32
#SBATCH --mem=200G

mkdir -p slurmout

############################################################
# READ ARGUMENTS PASSED TO SBATCH
############################################################
var_name="$1"
START_YEARMONTH="$2"
END_YEARMONTH="$3"

if [ -z "$var_name" ] || [ -z "$START_YEARMONTH" ] || [ -z "$END_YEARMONTH" ]; then
    echo "ERROR: Usage: sbatch jobsub_process_and_write_to_zarr.slurm <var_name> <start_yearmonth> <end_yearmonth>"
    exit 1
fi

############################################################
# ACTIVATE ENVIRONMENT
############################################################
source /network/rit/lab/basulab/mambaforge/etc/profile.d/conda.sh
conda activate /network/rit/lab/basulab/conda_envs/hb533188/DFSAI

echo "=========================================================="
echo "Running ICON-DREAM-Global processing"
echo "Variable: $var_name"
echo "Start: $START_YEARMONTH"
echo "End: $END_YEARMONTH"
echo "CPUs requested: $SLURM_CPUS_PER_TASK"
echo "=========================================================="

python process_and_write_to_zarr.py \
    --var_name "$var_name" \
    --start-yearmonth "$START_YEARMONTH" \
    --end-yearmonth "$END_YEARMONTH"
