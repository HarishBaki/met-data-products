#!/usr/bin/env bash
# =========================================
# Detect first date where 'atmos/' folder appears
# =========================================
if false; then
    set -euo pipefail

    BUCKET="s3://noaa-gfs-bdp-pds"
    START="2021-03-01"
    END="2021-04-01"

    echo "Detecting first date where 'atmos/' appears…"

    d="$START"
    found_date=""

    while [ "$(date -u -d "$d" +%s)" -le "$(date -u -d "$END" +%s)" ]; do
        ymd=$(date -u -d "$d" +%Y%m%d)

        cycle=00
        atmos_path="gfs.${ymd}/${cycle}/atmos/gfs.t${cycle}z.pgrb2.0p25.f000"
        echo "Checking: $atmos_path"

        # Try to HEAD the file (MUCH faster than GET)
        if aws s3api head-object \
            --no-sign-request \
            --bucket noaa-gfs-bdp-pds \
            --key "$atmos_path" >/dev/null 2>&1; then
            
            echo "First atmos folder detected: ${ymd} cycle ${cycle}Z"
            echo "Path: $atmos_path"
            exit 0
        fi

        d=$(date -I -u -d "$d + 1 day")
    done

    echo "No atmos folder found up to $END"
fi
# First atmos folder detected: 20210323 cycle 00Z

# =========================================
# Generate monthly download command lists
# =========================================

if false; then
    #!/usr/bin/env bash
    set -euo pipefail

    BUCKET="s3://noaa-gfs-bdp-pds"
    START="2021-01-01"
    END="2025-12-31"
    ATMOS_START="2021-03-23"

    OUT_BASE="/network/rit/lab/basulab/RAW_DATA/GFS0p25"
    LIST_DIR="./gfs_lists"
    mkdir -p "$OUT_BASE" "$LIST_DIR"

    echo "Building monthly download command lists..."
    echo "Legacy → before $ATMOS_START | Atmos → after"

    d="$START"
    cur_month=""

    while [ "$(date -d "$d" +%s)" -le "$(date -d "$END" +%s)" ]; do

        y=$(date -d "$d" +%Y)
        m=$(date -d "$d" +%m)
        day=$(date -d "$d" +%d)
        ymd="${y}${m}${day}"

        month_key="${y}-${m}"
        list_file="${LIST_DIR}/${month_key}.txt"

        # Create new file when month changes
        if [[ "$month_key" != "$cur_month" ]]; then
            echo "Creating list for $month_key → $list_file"
            echo -n "" > "$list_file"
            cur_month="$month_key"
        fi

        # Determine structure
        if [ "$(date -d "$d" +%s)" -lt "$(date -d "$ATMOS_START" +%s)" ]; then
            STRUCT="LEGACY"
        else
            STRUCT="ATMOS"
        fi

        for cycle in 00 06 12 18; do

            # Build correct prefix
            if [[ "$STRUCT" == "LEGACY" ]]; then
                prefix="${BUCKET}/gfs.${ymd}/${cycle}/"
            else
                prefix="${BUCKET}/gfs.${ymd}/${cycle}/atmos/"
            fi

            dest="${OUT_BASE}/${ymd}/${cycle}/"

            # Append to monthly command list
            echo "cp -n ${prefix}gfs.t${cycle}z.pgrb2.0p25.f000 ${dest}" >> "$list_file"

        done

        d=$(date -I -d "$d + 1 day")
    done

    echo "Command list generation complete."
fi

# =========================================
# Execute download command lists using s5cmd
# =========================================

#!/usr/bin/env bash
set -u #eo pipefail

JOBDIR="gfs_lists"

for jobfile in ${JOBDIR}/*.txt; do
    echo "======================================"
    echo " Running jobfile: $jobfile"
    echo "======================================"

    s5cmd --no-sign-request --numworkers 32 --stat run "$jobfile"

    echo ""
    echo " Finished: $jobfile"
    echo ""
done