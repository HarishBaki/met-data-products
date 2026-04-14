#!/bin/bash

SCRIPT_DIR="$(cd -- "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"

# Parameters
BUCKET="s3://noaa-mrms-pds"
OUTDIR="$REPO_ROOT/data/MRMS_grib_data"

START=20201014
END=20250826

if false; then
    # -------- Downloading MRMS data --------
    for PRODUCT in \
        "CONUS/MergedReflectivityAtLowestAltitude_00.50" \
        "CONUS/MergedReflectivityQCComposite_00.50"
    do
        echo ">> Downloading $PRODUCT"
        # Loop by day (uses GNU date; on mac: gdate)
        d="$START"
        while [ "$d" -le "$END" ]; do
            echo ">> $d"

            mkdir -p "$OUTDIR/$PRODUCT/$d/source"
            s5cmd --no-sign-request --numworkers 32 sync "$BUCKET/$PRODUCT/$d/*.grib2.gz" "$OUTDIR/$PRODUCT/$d/source/"

            d=$(date -d "$d + 1 day" +%Y%m%d)
        done
    done

    # -------- Checking missing dates --------
    for PRODUCT in \
        "CONUS/MergedReflectivityAtLowestAltitude_00.50" \
        "CONUS/MergedReflectivityQCComposite_00.50"
    do
        #echo "   -> $PRODUCT"
        echo -n > "$OUTDIR/$PRODUCT/missing_dates.txt"

        d="$START"
        while [ "$d" -le "$END" ]; do
            #echo ">> Processing $d"

            SRCDIR="$OUTDIR/$PRODUCT/$d/source"

            file_count=$(ls "$SRCDIR"/*on_orog.nc 2>/dev/null | wc -l)
            if [ "$file_count" -eq 0 ]; then
                echo "   -> WARNING: Only $file_count files found in $SRCDIR for date $d"
                echo "$d" >> "$OUTDIR/$PRODUCT/missing_dates.txt"
            fi
            d=$(date -d "$d + 1 day" +%Y%m%d)
        done
    done
fi

# -------- Missing dates loop --------
for PRODUCT in \
    "CONUS/MergedReflectivityAtLowestAltitude_00.50" \
    "CONUS/MergedReflectivityQCComposite_00.50"
do
    MISSING_FILE="$OUTDIR/$PRODUCT/missing_dates.txt"
    if [ -f "$MISSING_FILE" ]; then
        echo ">> Downloading missing dates for $PRODUCT"
        while read -r d; do
            echo "   -> $d"
            mkdir -p "$OUTDIR/$PRODUCT/$d/source"
            s5cmd --no-sign-request --numworkers 32 sync "$BUCKET/$PRODUCT/$d/*.grib2.gz" "$OUTDIR/$PRODUCT/$d/source/"
        done < "$MISSING_FILE"
    fi
done
