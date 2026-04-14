#!/bin/bash

# ==============================
# CONFIGURATION
# ==============================
MAX_PARALLEL=5
METHOD="bilinear"
INTENDED_LR_DATA="EDDE"  # set to ERA5/EDDE/ICON for LR, or leave empty for HR

VARS=(
    si10
    tp
    i10fg
    t2m
    sp
    d2m
    u10
    v10
    wdir10
    fsr
)

# ==============================
# MAIN LOOP
# ==============================
#for year in {2018..2025}; do
    PROCESS_START="201001"
    PROCESS_END="202512"
    for VAR in "${VARS[@]}"; do
        while [ "$(squeue -u "$USER" -n ICON-DREAM-regrid -h | wc -l)" -ge "$MAX_PARALLEL" ]; do
            echo "Reached $MAX_PARALLEL jobs running. Waiting..."
            sleep 30
        done

        echo "Submitting: $VAR  $PROCESS_START to $PROCESS_END  method=$METHOD  lr=$INTENDED_LR_DATA"
        if [ -z "$INTENDED_LR_DATA" ]; then
            sbatch jobsub_regrid_to_urma_zarr.slurm \
                "$VAR" \
                "$PROCESS_START" \
                "$PROCESS_END" \
                "$METHOD"
        else
            sbatch jobsub_regrid_to_urma_zarr.slurm \
                "$VAR" \
                "$PROCESS_START" \
                "$PROCESS_END" \
                "$METHOD" \
                "$INTENDED_LR_DATA"
        fi

        sleep 1
    done
#done

echo "=============================================="
echo "All jobs submitted!"
echo "=============================================="
