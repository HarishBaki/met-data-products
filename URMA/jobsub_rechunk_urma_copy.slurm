#!/bin/bash
#SBATCH --job-name=URMA-rechunk-copy
#SBATCH --output=slurmout/URMA-rechunk-copy-%j.out
#SBATCH --error=slurmout/URMA-rechunk-copy-%j.err
#SBATCH --time=02:00:00
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=8
#SBATCH --mem=32G
#SBATCH --propagate=NONE

# One variable/year rechunk-copy unit -- see rechunk_urma_copy.py and the
# 2026-09 URMA 6h->24h rechunk plan. Pure I/O rechunk-copy (no GRIB parsing),
# so lighter than jobsub_process_and_write_to_zarr.slurm's 32cpu/200G.
#
# Usage: STORE_SUFFIX=_rechunk24h REGION=New_Mexico sbatch jobsub_rechunk_urma_copy.slurm <variable> <year>

mkdir -p slurmout

source /network/rit/lab/basulab/mambaforge/etc/profile.d/conda.sh
conda activate DFSAI

variable="$1"
year="$2"
region="${REGION:?REGION must be set (e.g. REGION=New_Mexico)}"
store_suffix="${STORE_SUFFIX:?STORE_SUFFIX must be set (e.g. STORE_SUFFIX=_rechunk24h)}"

if [ -z "$variable" ] || [ -z "$year" ]; then
    echo "ERROR: Usage: sbatch jobsub_rechunk_urma_copy.slurm <variable> <year>"
    exit 1
fi

echo "=========================================================="
echo "Rechunk copy: variable=$variable year=$year region=$region suffix=$store_suffix"
echo "=========================================================="

python rechunk_urma_copy.py \
    --region "$region" \
    --variable "$variable" \
    --year "$year" \
    --store-suffix "$store_suffix" \
    --time-chunk 24
exit_code=$?

if [ "$exit_code" -ne 0 ]; then
    echo "$(date -Iseconds) job=${SLURM_JOB_ID:-unknown} node=${SLURMD_NODENAME:-unknown} var=$variable year=$year region=$region exit=$exit_code" >> rechunk_failed_jobs.log
fi

exit "$exit_code"
