#!/bin/bash
#SBATCH --job-name=MRMS
#SBATCH --output=slurmout/MRMS-%j.out
#SBATCH --error=slurmout/MRMS-%j.err
#SBATCH --time=02:00:00
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=32

SCRIPT_DIR="$(cd -- "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"

# --- Activate conda ---
source /network/rit/lab/basulab/mambaforge/etc/profile.d/conda.sh
conda activate deepexwind
cd "$REPO_ROOT"

# ============================================================
# Arguments:
#   $1 = DATE (YYYYMMDD)
# ============================================================

data_type=$1
DATE=$2

if [ -z "$DATE" ]; then
    echo "Usage: $0 YYYYMMDD"
    exit 1
fi

echo "=========================================================="
echo "Running MRMS daily processing"
echo "Data Type: $data_type"
echo "Date: $DATE"
echo "=========================================================="

python "$REPO_ROOT/scripts/data_processing/Process_and_write_to_zarr_MRMS_daily.py" --data_type "$data_type" --day "$DATE"
