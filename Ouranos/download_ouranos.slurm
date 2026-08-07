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
# download_ouranos.py has no built-in variable list (it's a generic THREDDS
# downloader, reused by other projects besides this one) - the DFS downscaling
# pipeline's variable choice lives here, as the VARS default below. Override
# with VARS (comma list), VARS_FILE (path to a vars list file), or
# VARS_FROM_CATALOG (any non-empty value - per-row vars_{frequency} lookup,
# read off CATALOG/--catalog, which already defaults to catalog_with_vars.csv)
# if a job needs a different set.
#
# REGION (default: New_York) selects configs/regions/{region}.yaml, which
# supplies the download bbox (grid.Ouranos.bbox) and, unless DEST_ROOT is set
# explicitly, the output location too.
#
# Examples:
#   # fx (static) variables - orog, areacella, sftlf, etc. - are rejected by
#   # download_ouranos.py; use download_fx.py instead, e.g.:
#   #   python download_fx.py --index 0 --vars orog,areacella
#
#   # Download the default DFS variable set, yearly, for CNRM-ESM2-1 historical (index 2)
#   sbatch --array=2 download_ouranos.slurm
#
#   # Download a specific year range for indices 11-15
#   START_YEAR=1985 END_YEAR=2014 sbatch --array=11-15 download_ouranos.slurm
#
#   # Download exactly what discover_vars.py found available, per row
#   VARS_FROM_CATALOG=1 sbatch --array=0-26 download_ouranos.slurm
#
#   # Download for New Mexico instead of the default New York
#   REGION=New_Mexico sbatch --array=0-26 download_ouranos.slurm
# ---------------------------------------------------------------------------

region="${REGION:?REGION must be set (e.g. REGION=New_Mexico) -- no default, to avoid silently running against the wrong region}"
dest_root="${DEST_ROOT:-}"
# DFS downscaling pipeline's variable set (matches process_and_write_to_zarr.py's
# VAR_GROUPS source_vars: tas->t2m ps->sp huss->sh2 pr->tp hurs->rh2 uas->u10 vas->v10)
vars="${VARS:-tas,hurs,huss,uas,vas,pr,ps}"
vars_file="${VARS_FILE:-}"
vars_from_catalog="${VARS_FROM_CATALOG:-}"   # any non-empty value enables this mode
catalog="${CATALOG:-}"
time_chunk="${TIME_CHUNK:-year}"
start_year="${START_YEAR:-}"
end_year="${END_YEAR:-}"
start_date="${START_DATE:-}"
end_date="${END_DATE:-}"
num_workers="${NUM_WORKERS:-${SLURM_CPUS_PER_TASK:-4}}"
full_domain="${FULL_DOMAIN:-}"   # set to any non-empty value to enable

# Exactly one variable-source flag - VARS_FROM_CATALOG / VARS_FILE win over the VARS default
if [[ -n "${vars_from_catalog}" ]]; then
    var_args="--vars-from-catalog"
elif [[ -n "${vars_file}" ]]; then
    var_args="--vars-file ${vars_file}"
else
    var_args="--vars ${vars}"
fi

# Build optional flags
opt_args=""
[[ -n "${catalog}"     ]] && opt_args+=" --catalog    ${catalog}"
[[ -n "${start_year}"  ]] && opt_args+=" --start-year ${start_year}"
[[ -n "${end_year}"    ]] && opt_args+=" --end-year   ${end_year}"
[[ -n "${start_date}"  ]] && opt_args+=" --start-date ${start_date}"
[[ -n "${end_date}"    ]] && opt_args+=" --end-date   ${end_date}"
[[ -n "${full_domain}" ]] && opt_args+=" --full-domain"
[[ -n "${dest_root}"   ]] && opt_args+=" --dest-root  ${dest_root}"

echo "Config: index=${SLURM_ARRAY_TASK_ID} region=${region} dest=${dest_root:-<region-derived>} ${var_args} chunk=${time_chunk}${opt_args}"

python download_ouranos.py \
    --index       "${SLURM_ARRAY_TASK_ID}" \
    --region      "${region}" \
    ${var_args} \
    --time-chunk  "${time_chunk}" \
    --num-workers "${num_workers}" \
    ${opt_args}

echo "Job end: $(date)"
