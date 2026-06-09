#!/bin/bash
#SBATCH --job-name=download_ouranos
#SBATCH --output=slurmout/download_ouranos-%A_%a.out
#SBATCH --error=slurmout/download_ouranos-%A_%a.err
#SBATCH --time=05-00:00:00
#SBATCH --nodes=1
#SBATCH --ntasks-per-node=1
#SBATCH --cpus-per-task=4
#SBATCH --array=0-26        # one task per catalog row; override with --array=N or N-M

mkdir -p slurmout

source /network/rit/lab/basulab/mambaforge/etc/profile.d/conda.sh
conda activate DFSAI

echo "Job start: $(date)  array_task=${SLURM_ARRAY_TASK_ID}"

# ---------------------------------------------------------------------------
# Configuration — override any of these via env vars before sbatch
#
# Examples:
#   # Download orography for all 27 products in parallel
#   VARS=static TIME_CHUNK=static FULL_DOMAIN=1 \
#       sbatch --array=0-26 download_ouranos.slurm
#
#   # Download dynamic vars yearly for CNRM-ESM2-1 historical (index 2)
#   sbatch --array=2 download_ouranos.slurm
#
#   # Download a specific year range for indices 11-15
#   START_YEAR=1985 END_YEAR=2014 sbatch --array=11-15 download_ouranos.slurm
# ---------------------------------------------------------------------------

dest_root="${DEST_ROOT:-/network/rit/lab/basulab/RAW_DATA/Ouranos/1hr}"
vars="${VARS:-all}"
time_chunk="${TIME_CHUNK:-year}"
start_year="${START_YEAR:-}"
end_year="${END_YEAR:-}"
start_date="${START_DATE:-}"
end_date="${END_DATE:-}"
num_workers="${NUM_WORKERS:-${SLURM_CPUS_PER_TASK:-4}}"
full_domain="${FULL_DOMAIN:-}"   # set to any non-empty value to enable

# Build optional flags
opt_args=""
[[ -n "${start_year}"  ]] && opt_args+=" --start-year ${start_year}"
[[ -n "${end_year}"    ]] && opt_args+=" --end-year   ${end_year}"
[[ -n "${start_date}"  ]] && opt_args+=" --start-date ${start_date}"
[[ -n "${end_date}"    ]] && opt_args+=" --end-date   ${end_date}"
[[ -n "${full_domain}" ]] && opt_args+=" --full-domain"

echo "Config: index=${SLURM_ARRAY_TASK_ID} dest=${dest_root} vars=${vars} chunk=${time_chunk}${opt_args}"

python download_ouranos.py \
    --index       "${SLURM_ARRAY_TASK_ID}" \
    --dest-root   "${dest_root}" \
    --vars        "${vars}" \
    --time-chunk  "${time_chunk}" \
    --num-workers "${num_workers}" \
    ${opt_args}

echo "Job end: $(date)"
