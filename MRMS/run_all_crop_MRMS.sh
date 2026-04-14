#!/bin/bash
SCRIPT_DIR="$(cd -- "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
# conda activate cdo
# Parameters
OUTDIR="$REPO_ROOT/data/MRMS_grib_data"
TARGET_GRID="$REPO_ROOT/data/orography_grid_cf.nc"

if false; then
    START=20240217 # Start date in YYYYMMDD format
    END=20250826 # End date in YYYYMMDD format
    MAXJOBS=128

    for PRODUCT in \
        "CONUS/MergedReflectivityAtLowestAltitude_00.50" \
        "CONUS/MergedReflectivityQCComposite_00.50"
    do
        echo "   -> $PRODUCT"

        d="$START"
        while [ "$d" -le "$END" ]; do
            echo ">> Processing $d"

            SRCDIR="$OUTDIR/$PRODUCT/$d/source"
            UNZIPDIR="$OUTDIR/$PRODUCT/$d/unzipped"
            CROPDIR="$OUTDIR/$PRODUCT/$d/cropped_NYS"

            mkdir -p "$UNZIPDIR" "$CROPDIR"

            njobs=0

            # --- Step 1: unzip ---
            for f in "$SRCDIR"/*.grib2.gz; do
                [ -f "$f" ] || continue
                fname=$(basename "$f" .gz)
                gunzip -c "$f" > "$UNZIPDIR/$fname" &

                ((njobs++))
                if (( njobs % MAXJOBS == 0 )); then
                    wait
                fi
            done
            wait

            njobs=0

            # --- Step 2: interpolate ---
            for f in "$UNZIPDIR"/*.grib2; do
                [ -f "$f" ] || continue
                fname=$(basename "$f" .grib2)
                cdo -f nc4c remapbil,"$TARGET_GRID" "$f" "$CROPDIR/${fname}_on_orog.nc" &

                ((njobs++))
                if (( njobs % MAXJOBS == 0 )); then
                    wait
                fi
            done
            wait

            # --- Step 3: cleanup ---
            echo "   -> Cleaning up $SRCDIR"
            rm -rf "$SRCDIR"
            
        done
        
        d=$(date -d "$d + 1 day" +%Y%m%d)
    done
fi

# -------- Missing dates loop --------
MAXJOBS=64
for PRODUCT in \
    "CONUS/MergedReflectivityAtLowestAltitude_00.50" \
    "CONUS/MergedReflectivityQCComposite_00.50"
do
    MISSING_FILE="$OUTDIR/$PRODUCT/missing_dates.txt"
    if [ -f "$MISSING_FILE" ]; then
        echo ">> Downloading missing dates for $PRODUCT"
        while read -r d; do
            echo ">> Processing $d"
            SRCDIR="$OUTDIR/$PRODUCT/$d/source"
            UNZIPDIR="$OUTDIR/$PRODUCT/$d/unzipped"
            CROPDIR="$OUTDIR/$PRODUCT/$d/cropped_NYS"

            mkdir -p "$UNZIPDIR" "$CROPDIR"

            njobs=0

            # --- Step 1: unzip ---
            for f in "$SRCDIR"/*.grib2.gz; do
                [ -f "$f" ] || continue
                fname=$(basename "$f" .gz)
                gunzip -c "$f" > "$UNZIPDIR/$fname" &

                ((njobs++))
                if (( njobs % MAXJOBS == 0 )); then
                    wait
                fi
            done
            wait

            njobs=0

            # --- Step 2: interpolate ---
            for f in "$UNZIPDIR"/*.grib2; do
                [ -f "$f" ] || continue
                fname=$(basename "$f" .grib2)
                cdo -f nc4c remapbil,"$TARGET_GRID" "$f" "$CROPDIR/${fname}_on_orog.nc" &

                ((njobs++))
                if (( njobs % MAXJOBS == 0 )); then
                    wait
                fi
            done
            wait

            # --- Step 3: cleanup ---
            echo "   -> Cleaning up $SRCDIR"
            rm -rf "$SRCDIR"
        done < "$MISSING_FILE"
    fi
done
