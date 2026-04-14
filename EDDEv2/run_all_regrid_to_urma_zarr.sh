#!/bin/bash

# ==============================
# CONFIGURATION
# ==============================
MAX_PARALLEL=6
METHOD="bilinear"
INTENDED_LR_DATA=""  # set to ERA5/EDDE/ICON for LR, or leave empty for HR
RUN_TYPE="SSP3-7.0"      # Historical, SSP2-4.5, SSP3-7.0
PROCESS_START="202501"
PROCESS_END="203012"

VARS=(
    si10
    wdir10
    u10
    v10
    t2m
    d2m
    sp
    tp
)

# ==============================
# MAIN LOOP
# ==============================
for VAR in "${VARS[@]}"; do
    while [ "$(squeue -u "$USER" -n EDDEv2-regrid -h | wc -l)" -ge "$MAX_PARALLEL" ]; do
        echo "Reached $MAX_PARALLEL jobs running. Waiting..."
        sleep 30
    done

    echo "Submitting: run=$RUN_TYPE var=$VAR $PROCESS_START to $PROCESS_END method=$METHOD lr=$INTENDED_LR_DATA"
    if [ -z "$INTENDED_LR_DATA" ]; then
        sbatch jobsub_regrid_to_urma_zarr.slurm \
            "$RUN_TYPE" \
            "$VAR" \
            "$PROCESS_START" \
            "$PROCESS_END" \
            "$METHOD"
    else
        sbatch jobsub_regrid_to_urma_zarr.slurm \
            "$RUN_TYPE" \
            "$VAR" \
            "$PROCESS_START" \
            "$PROCESS_END" \
            "$METHOD" \
            "$INTENDED_LR_DATA"
    fi

    sleep 1
done

echo "=============================================="
echo "All jobs submitted!"
echo "=============================================="

