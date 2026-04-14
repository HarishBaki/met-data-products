#!/bin/bash
# ============================================================
# Script: download_missing.sh
# Usage : ./download_missing.sh
# Purpose: Loop over all YEAR folders in data/CONUS404/,
#          identify files smaller than 5 MB, extract dates,
#          and generate a single missing_dates_all.txt file.
#          Then submit an array job to re-download them.
# ============================================================

# Ensure log directory exists
mkdir -p slurmout
MAX_ARRAY=300   # adjust if your cluster has a higher limit

BASE_DIR="data/CONUS404"
OUTPUT_FILE="missing_dates_all.txt"

if [ ! -d "$BASE_DIR" ]; then
    echo "Directory $BASE_DIR does not exist!"
    exit 1
fi

echo "Scanning $BASE_DIR for missing or corrupted files (<5 MB)..."

# Empty old output file
> "$OUTPUT_FILE"

for YEAR_DIR in "$BASE_DIR"/*/; do
    YEAR=$(basename "$YEAR_DIR")
    echo "Checking year $YEAR ..."

    # Find small files, extract YYYY-MM-DD, and append to unified list
    find "$YEAR_DIR" -maxdepth 1 -type f -size -5M -printf "%f\n" \
        | sed -E 's/.*([0-9]{4}-[0-9]{2}-[0-9]{2}).*/\1/' \
        >> "$OUTPUT_FILE"
done

# Sort and unique the final list
sort -u "$OUTPUT_FILE" -o "$OUTPUT_FILE"

if [ -s "$OUTPUT_FILE" ]; then
    echo "Missing/corrupted files detected across all years:"
    cat "$OUTPUT_FILE"
    echo "Unified list saved to $OUTPUT_FILE"

    # Count how many missing days
    DAYS=$(wc -l < "$OUTPUT_FILE")

    if [ "$DAYS" -le "$MAX_ARRAY" ]; then
        sbatch --array=0-$((DAYS-1)) jobsub_conus_download.slurm "$OUTPUT_FILE" file
    else
        echo "Splitting into chunks of $MAX_ARRAY..."
        START=0
        while [ $START -lt $DAYS ]; do
            END=$(( START + MAX_ARRAY - 1 ))
            if [ $END -ge $((DAYS-1)) ]; then
                END=$((DAYS-1))
            fi
            echo "Submitting chunk: $START-$END"
            sbatch --array=$START-$END jobsub_conus_download.slurm "$OUTPUT_FILE" file
            START=$((END+1))
        done
    fi

else
    echo "No missing or corrupted files found."
    rm -f "$OUTPUT_FILE"
fi