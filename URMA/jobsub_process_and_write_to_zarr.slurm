#!/bin/bash
#SBATCH --job-name=URMA
#SBATCH --output=slurmout/URMA-%j.out
#SBATCH --error=slurmout/URMA-%j.err
#SBATCH --time=24:00:00
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=32
#SBATCH --mem=200G # On DGX/HPC, one CPU is 8G/2G, so 32 CPUs is 256G/64G, but we are under requesting to be safe. 
#SBATCH --propagate=NONE

mkdir -p slurmout

############################################################
# READ ARGUMENTS PASSED TO SBATCH
#
# Override REGION via env var before sbatch (default: New_York), e.g.:
#   REGION=New_Mexico sbatch jobsub_process_and_write_to_zarr.slurm t2m 2025
############################################################
var_name="$1"
year="$2"
region="${REGION:?REGION must be set (e.g. REGION=New_Mexico) -- no default, to avoid silently running against the wrong region}"

if [ -z "$var_name" ] || [ -z "$year" ]; then
    echo "ERROR: Usage: sbatch run_urma.sh <var_name> <year>"
    exit 1
fi

############################################################
# ACTIVATE ENVIRONMENT
############################################################
source /network/rit/lab/basulab/mambaforge/etc/profile.d/conda.sh
conda activate DFSAI

echo "=========================================================="
echo "Running URMA yearly processing"
echo "Variable: $var_name"
echo "Year: $year"
echo "Region: $region"
echo "CPUs requested: $SLURM_CPUS_PER_TASK"
echo "=========================================================="

python process_and_write_to_zarr.py \
    --var_name "$var_name" \
    --year "$year" \
    --region "$region"
exit_code=$?

# Plain-file failure log, independent of sacct -- its-head has no sacct access at all
# (dgx's is unusable cross-cluster too), so this is the only way to detect a failed job
# there. Appends one line per failure; safe under concurrent jobs since each open+write
# is small and append-only. Check with: tail -f failed_jobs.log
if [ "$exit_code" -ne 0 ]; then
    echo "$(date -Iseconds) job=${SLURM_JOB_ID:-unknown} node=${SLURMD_NODENAME:-unknown} var=$var_name year=$year region=$region exit=$exit_code" >> failed_jobs.log
fi

exit "$exit_code"