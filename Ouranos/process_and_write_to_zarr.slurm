#!/bin/bash
#SBATCH --job-name=process_ouranos
#SBATCH --output=slurmout/process_ouranos-%j.out
#SBATCH --error=slurmout/process_ouranos-%j.err
#SBATCH --time=02:00:00
#SBATCH --nodes=1
#SBATCH --ntasks-per-node=1
#SBATCH --cpus-per-task=2
#SBATCH --mem=16G

mkdir -p slurmout

source /network/rit/lab/basulab/mambaforge/etc/profile.d/conda.sh
conda activate DFSAI

############################################################
# Usage: sbatch process_and_write_to_zarr.slurm <index> <var|init> [year]
#   <index>  catalog row (0-26)
#   <var>    one of t2m sp sh2 tp rh2 u10 v10 si10 wdir10, or "init"
#   [year]   required unless <var> is "init"
#
# Override OUTPUT_ROOT / RAW_ROOT / KEEP_RAW via env vars before sbatch.
############################################################
index="$1"
var="$2"
year="$3"

if [[ -z "${index}" || -z "${var}" ]]; then
    echo "ERROR: Usage: sbatch process_and_write_to_zarr.slurm <index> <var|init> [year]"
    exit 1
fi

output_root="${OUTPUT_ROOT:-/network/rit/lab/basulab/Projects/DFS/DATA/Ouranos_NYS}"
raw_root="${RAW_ROOT:-/network/rit/lab/basulab/RAW_DATA/Ouranos/1hr}"
keep_raw="${KEEP_RAW:-}"   # set to any non-empty value to keep downloaded yearly NetCDF files

opt_args=""
[[ -n "${year}"     ]] && opt_args+=" --year ${year}"
[[ -n "${keep_raw}" ]] && opt_args+=" --keep-raw"

echo "Job start: $(date)  index=${index} var=${var} year=${year}"

python process_and_write_to_zarr.py \
    --index       "${index}" \
    --var         "${var}" \
    --output-root "${output_root}" \
    --raw-root    "${raw_root}" \
    ${opt_args}

echo "Job end: $(date)"
