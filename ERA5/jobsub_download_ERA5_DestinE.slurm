#!/bin/bash
#SBATCH --job-name=ERA5_DestinE
#SBATCH --output=slurmout/ERA5_DestinE-%j.out
#SBATCH --error=slurmout/ERA5_DestinE-%j.err
#SBATCH --time=02:00:00
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=64
#SBATCH --mem=374G

mkdir -p slurmout

############################################################
# READ ARGUMENTS PASSED TO SBATCH
############################################################
START_DATE="${1:-2010}"
END_DATE="${2:-2025}"
VAR_LIST="${3:-u10 v10 si10 wdir10 t2m d2m tp sp}"
OVERWRITE_FLAG="$4"

############################################################
# ACTIVATE ENVIRONMENT
############################################################
source /network/rit/lab/basulab/mambaforge/etc/profile.d/conda.sh
conda activate DFSAI

echo "=========================================================="
echo "Running ERA5 Destination Earth download"
echo "Start date: $START_DATE"
echo "End date: $END_DATE"
echo "Variables: $VAR_LIST"
echo "Overwrite flag: $OVERWRITE_FLAG"
echo "CPUs requested: $SLURM_CPUS_PER_TASK"
echo "=========================================================="

python download_ERA5_DestinE.py \
    --start "$START_DATE" \
    --end "$END_DATE" \
    --vars $VAR_LIST \
    $OVERWRITE_FLAG
