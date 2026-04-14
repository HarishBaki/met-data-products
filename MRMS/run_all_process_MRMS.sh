#!/bin/bash

SCRIPT_DIR="$(cd -- "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"

MAX_PARALLEL=8   # Limit: 32 jobs at a time
START=20201101 # Start date 20201015 in YYYYMMDD format
END=20250826   # End date 20250826 in YYYYMMDD format

for PRODUCT in \
    "MergedReflectivityAtLowestAltitude_00.50" \
    "MergedReflectivityQCComposite_00.50"
do
    echo "   -> $PRODUCT"

    d="$START"
    while [ "$d" -le "$END" ]; do
        echo ">> Processing $d"

        CROPDIR="$REPO_ROOT/data/MRMS_grib_data/CONUS/$PRODUCT/$d/cropped_NYS"
        missing_dates_file="$REPO_ROOT/data/missing_dates_${PRODUCT//\//_}.txt"
        if [ ! -f "$missing_dates_file" ]; then
            touch "$missing_dates_file"
        fi

        file_count=$(ls "$CROPDIR"/*on_orog.nc 2>/dev/null | wc -l)
        if [ "$file_count" -ge 700 ]; then
            echo "   -> Enough files found ($file_count), submitting job..."
            sbatch "$REPO_ROOT/workflows/data_processing/jobsub_process_MRMS.slurm" "$PRODUCT" "$d"
        else
            echo "   -> Not enough files found ($file_count), skipping..."
            echo $d >> "$missing_dates_file"
        fi

        # --- Throttle to max parallel jobs ---
        while [ $(squeue -u $USER | grep -c MRMS) -ge $MAX_PARALLEL ]; do
            echo "Reached $MAX_PARALLEL jobs, waiting..."
            sleep 30   # wait 30 seconds before checking again
        done

        d=$(date -d "$d + 1 day" +%Y%m%d)
    done

    # After finishing the product, wait until ALL jobs done
    while [ $(squeue -u $USER | grep -c MRMS) -gt 0 ]; do
        echo "Waiting for PRODUCT=$PRODUCT jobs to finish..."
        sleep 60
    done

    # --- Clean missing dates file (sort + unique) ---
    if [ -f "$missing_dates_file" ]; then
        sort -u "$missing_dates_file" -o "$missing_dates_file"
        echo "Cleaned missing dates file: $missing_dates_file"
    fi

done

echo "All jobs submitted and completed."
