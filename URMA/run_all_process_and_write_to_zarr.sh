#!/bin/bash

# ==============================
# CONFIGURATION
# ==============================
MAX_PARALLEL=8        # Limit: 8 jobs running at once

# URMA variable list
VARS=(
    si10    # 10 m wind speed
    i10fg   # 10 m wind gust
    t2m     # 2 m air temperature
    sp      # surface pressure
    d2m     # 2 m dew point temperature
    u10     # 10 m eastward wind
    v10     # 10 m northward wind
    sh2     # 2 m specific humidity
    wdir10  # 10 m wind direction
    tp      # total precipitation
)

# Year range
START_YEAR=2014
END_YEAR=2025

# ==============================
# MAIN LOOP
# ==============================
for VAR in "${VARS[@]}"; do
    for YEAR in $(seq $START_YEAR $END_YEAR); do

        # Throttle submissions if too many active jobs
        while [ $(squeue -u $USER | grep -c URMA) -ge $MAX_PARALLEL ]; do
            echo "Reached $MAX_PARALLEL jobs running. Waiting..."
            sleep 30
        done

        echo "Submitting: $VAR  $YEAR"
        sbatch jobsub_process_and_write_to_zarr.slurm "$VAR" "$YEAR"

        sleep 1   # small delay for SLURM responsiveness

    done
done

echo "=============================================="
echo "All jobs submitted!"
echo "=============================================="