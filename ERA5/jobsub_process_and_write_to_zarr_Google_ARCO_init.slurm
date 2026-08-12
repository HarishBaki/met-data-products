#!/bin/bash
#SBATCH --job-name=ERA5_ARCO-init
#SBATCH --output=slurmout/ERA5_ARCO-init-%j.out
#SBATCH --error=slurmout/ERA5_ARCO-init-%j.err
#SBATCH --time=02:00:00
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=2
#SBATCH --mem=8G
#SBATCH --propagate=NONE
#SBATCH --exclude=dgx17,dgx18

# Serially pre-initializes the output zarr group/variable skeleton for every
# (group, var) pair passed below, one process at a time, inside a single
# SLURM job -- this is the step run_all_process_and_write_to_zarr_Google_ARCO.sh
# submits and waits on before fanning out the real parallel jobs, so multiple
# jobs never race to create the same store (see
# process_and_write_to_zarr_Google_ARCO.py --init-only). Especially important
# here: unlike other products, ERA5-ARCO's surface (sl) group writes to the
# SAME shared zarr store across every year, so all years' jobs for a given var
# race on the exact same store-creation step, not just same-year jobs.
#
# Entries are "group:var", e.g. "sl:t2m".
#
# Usage: sbatch jobsub_process_and_write_to_zarr_Google_ARCO_init.slurm <region> group1:var1 [group2:var2 ...]

set -euo pipefail

mkdir -p slurmout

source /network/rit/lab/basulab/mambaforge/etc/profile.d/conda.sh
export MAGPLUS_HOME="${MAGPLUS_HOME:-}"
conda activate /network/rit/lab/basulab/conda_envs/hb533188/DFSAI

REGION="$1"; shift

if [ -z "${REGION}" ] || [ "$#" -eq 0 ]; then
    echo "ERROR: Usage: sbatch jobsub_process_and_write_to_zarr_Google_ARCO_init.slurm <region> group1:var1 [...]"
    exit 1
fi

for ENTRY in "$@"; do
    GROUP="${ENTRY%%:*}"
    VAR_NAME="${ENTRY#*:}"
    cmd=(python process_and_write_to_zarr_Google_ARCO.py
        --var-name "${VAR_NAME}"
        --group "${GROUP}"
        --process-start "2020-01-01"
        --process-end "2020-01-01"
        --region "${REGION}"
        --init-only)
    [ "${GROUP}" = "pl" ] && cmd+=(--pressure-levels all)
    [ "${GROUP}" = "ml" ] && cmd+=(--model-levels 137)
    echo "[init] group=${GROUP} var_name=${VAR_NAME} region=${REGION}"
    "${cmd[@]}"
done
