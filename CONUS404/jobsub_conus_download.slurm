#!/bin/bash
#SBATCH --job-name=conus404_zarr
#SBATCH --output=slurmout/conus404_zarr-%j.out
#SBATCH --error=slurmout/conus404_zarr-%j.err
#SBATCH --time=02-00:00:00
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=32
#SBATCH --mem=80G

mkdir -p slurmout

############################################################
# READ ARGUMENTS PASSED TO SBATCH
# Usage:
#   sbatch jobsub_conus_download.slurm PROCESS_START PROCESS_END FULL_START FULL_END "VARS" [--overwrite]
# Examples:
#   sbatch jobsub_conus_download.slurm 2018-01-01 2018-12-31 1979 2025 "U10 V10"
#   sbatch jobsub_conus_download.slurm 2018-03-05 2018-03-05 1979 2025 "U10 V10" --overwrite
############################################################
PROCESS_START="${1:-1979-10-01}"
PROCESS_END="${2:-2022-12-31}"
FULL_START_YEAR="${3:-1979}"
FULL_END_YEAR="${4:-2030}"
VAR_LIST="${5:-U10 V10 T2 TD2 PSFC SRH03 USHR6 VSHR6 SBCAPE MLCAPE MUCAPE}"
OVERWRITE_FLAG="$6"

############################################################
# ACTIVATE ENVIRONMENT
############################################################
source /network/rit/lab/basulab/mambaforge/etc/profile.d/conda.sh
conda activate /network/rit/lab/basulab/conda_envs/hb533188/DFSAI


echo "=========================================================="
echo "Running CONUS404 direct-to-zarr write"
echo "Process start:   $PROCESS_START"
echo "Process end:     $PROCESS_END"
echo "Full start year: $FULL_START_YEAR"
echo "Full end year:   $FULL_END_YEAR"
echo "Variables:       $VAR_LIST"
echo "Overwrite flag:  $OVERWRITE_FLAG"
echo "CPUs requested:  $SLURM_CPUS_PER_TASK"
echo "n_jobs:          auto in python (min(12, requested CPUs))"
echo "=========================================================="

python conus404_downloader.py \
    --process-start "$PROCESS_START" \
    --process-end "$PROCESS_END" \
    --full-start-year "$FULL_START_YEAR" \
    --full-end-year "$FULL_END_YEAR" \
    --var-list $VAR_LIST \
    $OVERWRITE_FLAG
