#!/bin/bash

MAX_PARALLEL=7  # freetier QOS allows 8 jobs/user; stay one below the limit
METHOD="bilinear"
INTENDED_LR_DATA=""  # set to ERA5/EDDE/ICON for LR, or leave empty for HR

VARS=(
    d2m
    i10fg
    si10
    sp
    t2m
    tp
    u10
    v10
    wdir10
)

PROCESS_START="201801"
PROCESS_END="202512"

for VAR in "${VARS[@]}"; do
    while [ "$(squeue -u "$USER" --qos=freetier -h | wc -l)" -ge "$MAX_PARALLEL" ]; do
        echo "Reached $MAX_PARALLEL jobs under QOS freetier. Waiting..."
        sleep 30
    done

    echo "Submitting: $VAR  $PROCESS_START to $PROCESS_END  method=$METHOD  lr=${INTENDED_LR_DATA:-HR}"
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
