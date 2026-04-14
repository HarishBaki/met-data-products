#!/bin/bash

SCRIPT_DIR="$(cd -- "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
OUTDIR="$REPO_ROOT/data/MRMS_grib_data"
TARGET_GRID="$REPO_ROOT/data/orography_grid_cf.nc"

START=20201014 # Start date in YYYYMMDD format
END=20250826 # End date in YYYYMMDD format



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
        UNZIPDIR="$OUTDIR/$PRODUCT/$d/unzipped"
        CROPDIR="$OUTDIR/$PRODUCT/$d/cropped_NYS"

        file_count=$(ls "$CROPDIR"/*on_orog.nc 2>/dev/null | wc -l)
        if [ "$file_count" -eq 0 ]; then
            echo "   -> WARNING: Only $file_count files found in $CROPDIR for date $d"
            echo "$d" >> "$OUTDIR/$PRODUCT/missing_dates.txt"
        fi
        d=$(date -d "$d + 1 day" +%Y%m%d)
    done
done
