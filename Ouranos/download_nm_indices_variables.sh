#!/bin/bash
# Drive run_all_process_and_write_to_zarr.sh through the full data pull needed
# for the NM water-resources indices paper
# (Projects/Active/Climate-Downscaling/NM_multimodel_multiSSP_climate_characterization).
#
# This script does NOT orchestrate downloads itself -- it's a thin loop that
# calls run_all_process_and_write_to_zarr.sh once per (experiment, frequency)
# with that script's own INDICES/YEARS_SPEC/FREQUENCY/REGION/
# DOWNLOAD_VARS_OVERRIDE/DERIVED_VARS_OVERRIDE env-var overrides -- the last
# two trim each frequency's default variable group down to just what this
# paper needs (see DOWNLOAD_VARS_BY_FREQ below), skipping vars other Ouranos
# callers use but we don't. All real work (throttled sbatch submission,
# init-job-first, u10/v10-before-derived-vars dependency, skip-if-already-
# populated, final consolidate job) happens inside that script, unchanged.
#
# Domain: 4 GCMs x {historical, ssp245, ssp370} x {day, 1hr, 3hr} = 9 calls.
# Catalog row indices (catalog_with_vars.csv, verified 2026-08-23 -- all 12
# rows confirmed to carry every variable this paper needs at every frequency
# it needs them at):
#   historical (1950-2014): CNRM-ESM2-1=2  CanESM5=6  MPI-ESM1-2-LR=11 NorESM2-MM=23
#   ssp245     (2015-2100): CNRM-ESM2-1=4  CanESM5=8  MPI-ESM1-2-LR=17 NorESM2-MM=25
#   ssp370     (2015-2100): CNRM-ESM2-1=5  CanESM5=9  MPI-ESM1-2-LR=18 NorESM2-MM=26
#
# SAFETY: defaults to DRY_RUN=1, which prints each run_all_process_and_write_to_zarr.sh
# invocation without running it (that script itself has no dry-run mode -- it
# submits real sbatch jobs -- so this wrapper is what gives you a look before
# committing). Set DRY_RUN=0 to actually submit.
#
# Usage:
#   ./download_nm_indices_variables.sh                        # dry run (default, safe)
#   DRY_RUN=0 ./download_nm_indices_variables.sh               # submit all 9 passes
#   DRY_RUN=0 EXPERIMENTS=historical ./download_nm_indices_variables.sh        # just historical, all 3 freqs
#   DRY_RUN=0 FREQUENCIES=day ./download_nm_indices_variables.sh               # just day freq, all 3 experiments
#   DRY_RUN=0 MAX_PARALLEL=3 ./download_nm_indices_variables.sh                # thinner SLURM footprint
# (MAX_PARALLEL/CATALOG/OUTPUT_ROOT/RAW_ROOT etc. aren't handled specially here --
# set them in the environment before calling this script and they propagate
# straight through to run_all_process_and_write_to_zarr.sh like any inherited
# env var.)

set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")"

REGION="${REGION:-New_Mexico}"
DRY_RUN="${DRY_RUN:-1}"

declare -A INDICES_BY_EXPERIMENT=(
    [historical]="2 6 11 23"
    [ssp245]="4 8 17 25"
    [ssp370]="5 9 18 26"
)
declare -A YEARS_BY_EXPERIMENT=(
    [historical]="1950-2014"
    [ssp245]="2015-2100"
    [ssp370]="2015-2100"
)

# Trimmed to what this paper actually needs, beyond Paper structure.md M1.3's
# variable table: drops si10/wdir10 at 1hr (not downloadable at all -- Ouranos
# never archives wind speed/direction natively, only uas/vas -- and not used
# anywhere downstream either, since M3.1's Penman-Monteith wind term is
# computed from uas/vas directly, not from a stored derived variable) and
# clwvi/prw at 3hr, t2m/snw at day (redundant with another frequency). Keeps
# `sp` at 1hr (added back deliberately): M3.1's psychrometric constant uses
# actual surface pressure rather than the elevation-based approximation, at
# near-zero marginal download cost alongside the rest of the 1hr group.
declare -A DOWNLOAD_VARS_BY_FREQ=(
    [day]="tasmax tasmin"
    [1hr]="t2m sp tp rh2 sh2 u10 v10 rsds rlds"
    [3hr]="hfls mrro snw"
)

# Override as space-separated lists to run a subset.
EXPERIMENTS="${EXPERIMENTS:-historical ssp245 ssp370}"
FREQUENCIES="${FREQUENCIES:-day 1hr 3hr}"

echo "=============================================="
echo "Region        : ${REGION}"
echo "Experiments   : ${EXPERIMENTS}"
echo "Frequencies   : ${FREQUENCIES}"
echo "Dry run       : ${DRY_RUN} (set DRY_RUN=0 to actually submit)"
echo "=============================================="

for experiment in ${EXPERIMENTS}; do
    indices="${INDICES_BY_EXPERIMENT[$experiment]:-}"
    years="${YEARS_BY_EXPERIMENT[$experiment]:-}"
    if [[ -z "${indices}" ]]; then
        echo "WARNING: no INDICES configured for experiment '${experiment}', skipping" >&2
        continue
    fi
    for freq in ${FREQUENCIES}; do
        echo ""
        echo "--- experiment=${experiment} indices=[${indices}] years=${years} freq=${freq} ---"
        cmd=(env FREQUENCY="${freq}" REGION="${REGION}" INDICES="${indices}" YEARS_SPEC="${years}" \
             DOWNLOAD_VARS_OVERRIDE="${DOWNLOAD_VARS_BY_FREQ[$freq]}" DERIVED_VARS_OVERRIDE="" \
             ./run_all_process_and_write_to_zarr.sh)
        printf '%q ' "${cmd[@]}"; echo   # %q: safe to copy-paste (INDICES has embedded spaces)
        if [[ "${DRY_RUN}" == "0" ]]; then
            "${cmd[@]}"
        fi
    done
done

echo ""
echo "Done. (dry_run=${DRY_RUN})"
