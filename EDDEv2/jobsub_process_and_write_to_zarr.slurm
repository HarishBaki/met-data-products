#!/bin/bash
#SBATCH --job-name=EDDEv2
#SBATCH --output=slurmout/EDDEv2-%j.out
#SBATCH --error=slurmout/EDDEv2-%j.err
#SBATCH --time=24:00:00
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=32
#SBATCH --mem=64G # On DGX/HPC, one CPU is 8G/2G, so 32 CPUs is 256G/64G, but we are under requesting to be safe. 
#SBATCH --propagate=NONE

mkdir -p slurmout

############################################################
# READ ARGUMENTS PASSED TO SBATCH
#
# Override REGION via env var before sbatch (default: New_York), e.g.:
#   REGION=New_Mexico sbatch jobsub_process_and_write_to_zarr.slurm t2m 2025-01-01 2025-12-31
############################################################
var_name="$1"
PROCESS_START="$2"
PROCESS_END="$3"
RUN_TYPE="${4:-SSP2-4.5}"
REGION="${REGION:?REGION must be set (e.g. REGION=New_Mexico) -- no default, to avoid silently running against the wrong region}"

if [ -z "$var_name" ] || [ -z "$PROCESS_START" ] || [ -z "$PROCESS_END" ]; then
    echo "ERROR: Usage: sbatch jobsub_process_and_write_to_zarr.slurm <var_name> <process_start> <process_end> [run_type]"
    exit 1
fi

############################################################
# ACTIVATE ENVIRONMENT
############################################################
source /network/rit/lab/basulab/mambaforge/etc/profile.d/conda.sh
conda activate DFSAI

echo "=========================================================="
echo "Running EDDEv2 processing"
echo "Variable: $var_name"
echo "Process start: $PROCESS_START"
echo "Process end: $PROCESS_END"
echo "Run type: $RUN_TYPE"
echo "Region: $REGION"
echo "CPUs requested: $SLURM_CPUS_PER_TASK"
echo "=========================================================="

python process_and_write_to_zarr.py \
    --run-type "$RUN_TYPE" \
    --var-name "$var_name" \
    --process-start "$PROCESS_START" \
    --process-end "$PROCESS_END" \
    --region "$REGION"
