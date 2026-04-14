#!/bin/bash

# ==============================
# CONFIGURATION
# ==============================
MAX_PARALLEL=8        # Limit: 7 jobs running at once

# EDDEv2 variable list
VARS=(
    si10    # 10 m wind speed
    t2m     # 2 m air temperature
    sp      # surface pressure
    d2m     # 2 m dew point temperature
    u10     # 10 m eastward wind
    v10     # 10 m northward wind
    wdir10  # 10 m wind direction
    tp      # total precipitation
)

# EDDEv2 run types
RUN_TYPES=(
    Historical
    SSP2-4.5
    SSP3-7.0
)

# ==============================
# MAIN LOOP
# ==============================
for RUN_TYPE in "${RUN_TYPES[@]}"; do
    if [ "$RUN_TYPE" = "Historical" ]; then
        PROCESS_START="1985-01"
        PROCESS_END="2014-12"
    else
        PROCESS_START="2025-01"
        PROCESS_END="2030-12"   # FIXME, for full years up to 2100
    fi

    for VAR in "${VARS[@]}"; do

        # Throttle submissions if too many active jobs
        while [ "$(squeue -u "$USER" -h -n EDDEv2 | wc -l)" -ge "$MAX_PARALLEL" ]; do
            echo "Reached $MAX_PARALLEL jobs running. Waiting..."
            sleep 30
        done

        echo "Submitting: $RUN_TYPE $VAR  $PROCESS_START to $PROCESS_END"
        sbatch jobsub_process_and_write_to_zarr.slurm \
            "$VAR" \
            "$PROCESS_START" \
            "$PROCESS_END" \
            "$RUN_TYPE"

        sleep 1   # small delay for SLURM responsiveness

    done
done

echo "=============================================="
echo "All jobs submitted!"
echo "=============================================="
