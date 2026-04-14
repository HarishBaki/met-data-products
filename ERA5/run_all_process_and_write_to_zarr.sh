#!/bin/bash

# ==============================
# CONFIGURATION
# ==============================
MAX_PARALLEL=5        # Limit: 7 jobs running at once

# ERA5 variable list
VARS=(
    si10    # 10 m wind speed
    i10fg   # 10 m wind gust
    t2m     # 2 m air temperature
    sp      # surface pressure
    d2m     # 2 m dew point temperature
    u10     # 10 m eastward wind
    v10     # 10 m northward wind
    wdir10  # 10 m wind direction
    tp      # total precipitation
)

# Year range for processing months
PROCESS_START="2017-12"
PROCESS_END="2025-12"

# ==============================
# MAIN LOOP
# ==============================
for VAR in "${VARS[@]}"; do

    # Throttle submissions if too many active jobs
    while [ $(squeue -u $USER | grep -c ERA5) -ge $MAX_PARALLEL ]; do
        echo "Reached $MAX_PARALLEL jobs running. Waiting..."
        sleep 30
    done

    echo "Submitting: $VAR  $PROCESS_START to $PROCESS_END"
    sbatch jobsub_process_and_write_to_zarr.slurm \
        "$VAR" \
        "$PROCESS_START" \
        "$PROCESS_END"

    sleep 1   # small delay for SLURM responsiveness

done

echo "=============================================="
echo "All jobs submitted!"
echo "=============================================="
