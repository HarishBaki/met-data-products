#!/bin/bash
#SBATCH --job-name=HRRR-init
#SBATCH --output=slurmout/HRRR-init-%j.out
#SBATCH --error=slurmout/HRRR-init-%j.err
#SBATCH --time=02:00:00
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=2
#SBATCH --mem=8G

# Serially pre-initializes the output zarr store/variable skeleton for every
# var passed below, one process at a time, inside a single SLURM job -- this
# is the step run_all_process_and_write_to_zarr_HRRRzarr.sh submits and waits
# on before fanning out the real parallel per-var jobs, so multiple jobs never
# race to create the same store (see process_and_write_to_zarr_HRRRzarr.py
# --init-only). --init-only never opens source data, just writes zarr metadata,
# and is a single-process, single-threaded operation -- 2 cpus is plenty. (A
# prior slowdown in URMA's equivalent step -- observed taking minutes -- was
# misdiagnosed as CPU-bound; the real cause was data_utils/zarr_io.py's
# init_zarr() building a huge dask graph purely to write empty metadata, fixed
# there. Cpus were briefly bumped to 128 across all products to test that
# hypothesis, which it disproved.)
#
# Entries are "source:<var>" or "derived:<var>" -- pass source vars before
# derived vars so the store already exists (created by the first source
# entry) by the time a derived entry's --init-only runs, since the derived
# pipeline requires the store to already exist.
#
# --y-chunk/--x-chunk for source vars must match jobsub_process_and_write_to_zarr_HRRRzarr.slurm's
# own DEFAULT_Y_CHUNK=128/DEFAULT_X_CHUNK=144 (its deliberate sub-domain chunk
# for source vars, unset -- i.e. full-domain -- for derived vars): the on-disk
# zarr chunk grid is fixed at whatever this init step creates it with, so a
# mismatch here means every real source-var write later fails with
# "Specified Zarr chunks ... unaligned with backend chunks" (confirmed in
# production -- this init script previously omitted them entirely, defaulting
# to full-domain chunks that didn't match the real per-var writes).
#
# Usage: sbatch jobsub_process_and_write_to_zarr_HRRRzarr_init.slurm <region> <process_start> <process_end> source:<var1> [source:<var2> ...] [derived:<var> ...]

set -euo pipefail

mkdir -p slurmout

source /network/rit/lab/basulab/mambaforge/etc/profile.d/conda.sh
export MAGPLUS_HOME="${MAGPLUS_HOME:-}"
export PYTHONUNBUFFERED=1
conda activate /network/rit/lab/basulab/conda_envs/hb533188/DFSAI

REGION="$1"; PROCESS_START="$2"; PROCESS_END="$3"; shift 3

if [ -z "${REGION}" ] || [ "$#" -eq 0 ]; then
    echo "ERROR: Usage: sbatch jobsub_process_and_write_to_zarr_HRRRzarr_init.slurm <region> <process_start> <process_end> source:<var1> [...] [derived:<var> ...]"
    exit 1
fi

for ENTRY in "$@"; do
    MODE="${ENTRY%%:*}"
    VAR_NAME="${ENTRY#*:}"
    echo "[init] mode=${MODE} var_name=${VAR_NAME} region=${REGION}"
    cmd=(python process_and_write_to_zarr_HRRRzarr.py "${MODE}"
        --var-name "${VAR_NAME}"
        --process-start "${PROCESS_START}"
        --process-end "${PROCESS_END}"
        --region "${REGION}"
        --init-only)
    if [ "${MODE}" = "source" ]; then
        cmd+=(--y-chunk 128 --x-chunk 144)
    fi
    "${cmd[@]}"
done
