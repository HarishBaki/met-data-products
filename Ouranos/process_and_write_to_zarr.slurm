#!/bin/bash
#SBATCH --job-name=process_ouranos
#SBATCH --output=slurmout/process_ouranos-%j.out
#SBATCH --error=slurmout/process_ouranos-%j.err
#SBATCH --time=02:00:00
#SBATCH --nodes=1
#SBATCH --ntasks-per-node=1
#SBATCH --cpus-per-task=8
#SBATCH --mem=16G

mkdir -p slurmout

source /network/rit/lab/basulab/mambaforge/etc/profile.d/conda.sh
conda activate DFSAI

############################################################
# Usage: sbatch process_and_write_to_zarr.slurm <index> <var|init> [year]
#   <index>  catalog row (0-26)
#   <var>    valid choices depend on FREQUENCY - see VAR_GROUPS_BY_FREQUENCY
#            in process_and_write_to_zarr.py, or "init"
#   [year]   required unless <var> is "init"
#
# Override REGION / OUTPUT_ROOT / RAW_ROOT / KEEP_RAW / FREQUENCY via env vars
# before sbatch. REGION defaults to New_York (configs/regions/{region}.yaml
# supplies the download bbox and, unless OUTPUT_ROOT/RAW_ROOT are set
# explicitly, those locations too). FREQUENCY defaults to 1hr. The Python
# script's own --catalog default (catalog_with_vars.csv) drives the per-row
# availability check; pass CATALOG to point at a different file.
############################################################
index="$1"
var="$2"
year="$3"

if [[ -z "${index}" || -z "${var}" ]]; then
    echo "ERROR: Usage: sbatch process_and_write_to_zarr.slurm <index> <var|init> [year]"
    exit 1
fi

region="${REGION:-New_York}"
output_root="${OUTPUT_ROOT:-}"
raw_root="${RAW_ROOT:-}"
keep_raw="${KEEP_RAW:-}"   # set to any non-empty value to keep downloaded yearly NetCDF files
frequency="${FREQUENCY:-1hr}"
catalog="${CATALOG:-}"

opt_args=""
[[ -n "${year}"        ]] && opt_args+=" --year ${year}"
[[ -n "${keep_raw}"    ]] && opt_args+=" --keep-raw"
[[ -n "${catalog}"     ]] && opt_args+=" --catalog ${catalog}"
[[ -n "${output_root}" ]] && opt_args+=" --output-root ${output_root}"
[[ -n "${raw_root}"    ]] && opt_args+=" --raw-root ${raw_root}"

echo "Job start: $(date)  index=${index} var=${var} year=${year} freq=${frequency} region=${region}"

python process_and_write_to_zarr.py \
    --index       "${index}" \
    --var         "${var}" \
    --frequency   "${frequency}" \
    --region      "${region}" \
    ${opt_args}

echo "Job end: $(date)"
