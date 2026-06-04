#!/bin/bash

# ==============================
# CONFIGURATION
# ==============================
MAX_PARALLEL=7

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

#PROCESS_START="201001"
#PROCESS_END="202512"

# ==============================
# MAIN LOOP
# ==============================
for VAR in "${VARS[@]}"; do
    for year in {2025..2025}; do
        PROCESS_START="${year}01"
        PROCESS_END="${year}12"
        while [ "$(squeue -u "$USER" -n ICON-DREAM -h | wc -l)" -ge "$MAX_PARALLEL" ]; do
            echo "Reached $MAX_PARALLEL jobs running. Waiting..."
            sleep 30
        done

        echo "Submitting: $VAR  $PROCESS_START to $PROCESS_END"
        sbatch jobsub_process_and_write_to_zarr.slurm \
            "$VAR" \
            "$PROCESS_START" \
            "$PROCESS_END"

        sleep 1
    done
done

echo "=============================================="
echo "All jobs submitted!"
echo "=============================================="
