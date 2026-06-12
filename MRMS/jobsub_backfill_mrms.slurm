#!/bin/bash
#SBATCH --job-name=MRMS_backfill
#SBATCH --output=slurmout/MRMS_backfill-%j.out
#SBATCH --error=slurmout/MRMS_backfill-%j.err
#SBATCH --time=24:00:00
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=32
#SBATCH --mem=64G

SCRIPT_DIR="$(cd -- "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"
mkdir -p slurmout

source /network/rit/lab/basulab/mambaforge/etc/profile.d/conda.sh
conda activate /network/rit/lab/basulab/conda_envs/hb533188/DFSAI

############################################################
# Arguments: <product> <start_yyyymmdd> <end_yyyymmdd>
############################################################
PRODUCT="$1"
START="$2"
END="$3"

if [ -z "$PRODUCT" ] || [ -z "$START" ] || [ -z "$END" ]; then
    echo "ERROR: Usage: sbatch jobsub_backfill_mrms.slurm <product> <start_yyyymmdd> <end_yyyymmdd>"
    exit 1
fi

echo "=========================================================="
echo "MRMS backfill"
echo "Product: $PRODUCT"
echo "Range:   $START -> $END"
echo "=========================================================="

python process_mrms.py process \
    --variables "$PRODUCT" \
    --process-start "$START" \
    --process-end "$END"
