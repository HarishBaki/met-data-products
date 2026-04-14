#!/bin/bash
#SBATCH --job-name=URMA
#SBATCH --output=slurmout/URMA-%j.out
#SBATCH --error=slurmout/URMA-%j.err
#SBATCH --time=02:00:00
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=32
#SBATCH --mem=200G # On DGX/HPC, one CPU is 8G/2G, so 32 CPUs is 256G/64G, but we are under requesting to be safe. 

mkdir -p slurmout

############################################################
# READ ARGUMENTS PASSED TO SBATCH
############################################################
var_name="$1"
year="$2"

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
echo "CPUs requested: $SLURM_CPUS_PER_TASK"
echo "=========================================================="

python process_and_write_to_zarr.py \
    --var_name "$var_name" \
    --year "$year"