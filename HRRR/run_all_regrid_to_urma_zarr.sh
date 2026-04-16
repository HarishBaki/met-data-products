#!/bin/bash

MAX_PARALLEL=4
METHOD="bilinear"
INTENDED_LR_DATA=""  # set to ERA5/EDDE/ICON for LR, or leave empty for HR

VARS=(
    d2m
    i10fg
    sh2
    si10
    sp
    t2m
    tp
    u10
    v10
    wdir10
)

PROCESS_START="202301"
PROCESS_END="202312"

for VAR in "${VARS[@]}"; do
    while [ "$(squeue -u "$USER" -n HRRR-regrid -h | wc -l)" -ge "$MAX_PARALLEL" ]; do
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

echo "=============================================="
echo "All jobs submitted!"
echo "=============================================="
