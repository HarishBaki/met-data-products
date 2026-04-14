#!/usr/bin/env bash
set -euo pipefail

# ================ LIST AVAILABLE VARIABLES =================
# aws s3 ls \
#   s3://dataspace/OTDS.012020.4326.1/raster/ \
#   --endpoint-url https://opentopography.s3.sdsc.edu \
#   --no-sign-request
#                            PRE aspect-cosine/
#                            PRE aspect-sine/
#                            PRE aspect/
#                            PRE convergence/
#                            PRE cti/
#                            PRE dev-magnitude/
#                            PRE dev-scale/
#                            PRE dx/
#                            PRE dxx/
#                            PRE dxy/
#                            PRE dy/
#                            PRE dyy/
#                            PRE eastness/
#                            PRE elev-stdev/
#                            PRE geom/
#                            PRE northness/
#                            PRE pcurv/
#                            PRE rough-magnitude/
#                            PRE rough-scale/
#                            PRE roughness/
#                            PRE slope/
#                            PRE spi/
#                            PRE tcurv/
#                            PRE tpi/
#                            PRE tri/
#                            PRE vrm/
# ==========================================================
# =========== LIST OF FILES INSIDE A VARIABLE ==============
#aws s3 ls s3://dataspace/OTDS.012020.4326.1/raster/slope/slope --endpoint-url https://opentopography.s3.sdsc.edu --no-sign-request
# 2020-01-15 19:32:04 4407961252 slope_90M_n00e000.tar.gz
# 2020-01-15 19:33:22 3219576865 slope_90M_n00e030.tar.gz
# 2020-01-15 19:32:04 1710599095 slope_90M_n00e060.tar.gz
# 2020-01-15 19:35:03 2295201906 slope_90M_n00e090.tar.gz
# 2020-01-15 19:34:29  192279757 slope_90M_n00e120.tar.gz
# 2020-01-15 19:34:48    1326597 slope_90M_n00e150.tar.gz
# 2020-01-15 19:34:48 1913685433 slope_90M_n00w030.tar.gz
# 2020-01-15 19:35:03  295909954 slope_90M_n00w060.tar.gz
# 2020-01-15 19:35:23 1388278192 slope_90M_n00w090.tar.gz
# 2020-01-15 19:36:41  902772359 slope_90M_n00w120.tar.gz
# 2020-01-15 19:36:47    8478381 slope_90M_n00w180.tar.gz
# 2020-01-15 19:39:04 2964043080 slope_90M_n30e000.tar.gz
# 2020-01-15 19:40:14 4310917089 slope_90M_n30e030.tar.gz
# 2020-01-15 19:41:38 4683972714 slope_90M_n30e060.tar.gz
# 2020-01-15 19:42:51 4651946134 slope_90M_n30e090.tar.gz
# 2020-01-15 19:43:21 2245573412 slope_90M_n30e120.tar.gz
# 2020-01-15 19:42:51  224236181 slope_90M_n30e150.tar.gz
# 2020-01-15 19:43:05  818004186 slope_90M_n30w030.tar.gz
# 2020-01-15 19:43:21  142064669 slope_90M_n30w060.tar.gz
# 2020-01-15 19:46:02 2944853593 slope_90M_n30w090.tar.gz
# 2020-01-15 19:46:53 4557059543 slope_90M_n30w120.tar.gz
# 2020-01-15 19:46:02  938233990 slope_90M_n30w150.tar.gz
# 2020-01-15 19:46:53  128690501 slope_90M_n30w180.tar.gz
# 2020-01-15 19:47:02 1047022245 slope_90M_n60e000.tar.gz
# 2020-01-15 19:47:12 1365060795 slope_90M_n60e030.tar.gz
# 2020-01-15 19:48:05 1897318341 slope_90M_n60e060.tar.gz
# 2020-01-15 19:50:19 2534192470 slope_90M_n60e090.tar.gz
# 2020-01-15 19:50:10 1991026574 slope_90M_n60e120.tar.gz
# 2020-01-15 19:50:19 1406227042 slope_90M_n60e150.tar.gz
# 2020-01-15 19:51:29  792804336 slope_90M_n60w030.tar.gz
# 2020-01-15 19:53:35 2539586829 slope_90M_n60w060.tar.gz
# 2020-01-15 19:52:14 1726438430 slope_90M_n60w090.tar.gz
# 2020-01-15 19:53:36 2015307053 slope_90M_n60w120.tar.gz
# 2020-01-15 19:54:24 1580698004 slope_90M_n60w150.tar.gz
# 2020-01-15 19:55:28  931762164 slope_90M_n60w180.tar.gz
# 2020-01-15 19:57:53 2657823273 slope_90M_s30e000.tar.gz
# 2020-01-15 19:56:10 1483815651 slope_90M_s30e030.tar.gz
# 2020-01-15 19:57:53     222208 slope_90M_s30e060.tar.gz
# 2020-01-15 19:57:53  619020819 slope_90M_s30e090.tar.gz
# 2020-01-15 19:59:39 2713397229 slope_90M_s30e120.tar.gz
# 2020-01-15 19:58:29  166228997 slope_90M_s30e150.tar.gz
# 2020-01-15 19:58:55     200304 slope_90M_s30w030.tar.gz
# 2020-01-15 20:00:45 2936806551 slope_90M_s30w060.tar.gz
# 2020-01-15 20:01:15 2360004079 slope_90M_s30w090.tar.gz
# 2020-01-15 20:00:46    3172332 slope_90M_s30w120.tar.gz
# 2020-01-15 20:00:46    2962635 slope_90M_s30w150.tar.gz
# 2020-01-15 20:00:47    3720453 slope_90M_s30w180.tar.gz
# 2020-01-15 20:00:47  236008065 slope_90M_s60e000.tar.gz
# 2020-01-15 20:01:07    4046225 slope_90M_s60e030.tar.gz
# 2020-01-15 20:01:08    5454932 slope_90M_s60e060.tar.gz
# 2020-01-15 20:01:08  112880896 slope_90M_s60e090.tar.gz
# 2020-01-15 20:01:15  802592722 slope_90M_s60e120.tar.gz
# 2020-01-15 20:01:16  213167916 slope_90M_s60e150.tar.gz
# 2020-01-15 20:01:28     541570 slope_90M_s60w030.tar.gz
# 2020-01-15 20:01:28  259340000 slope_90M_s60w060.tar.gz
# 2020-01-15 20:01:42 1230057580 slope_90M_s60w090.tar.gz
# 2020-01-15 20:01:53     709752 slope_90M_s60w180.tar.gz
# ==========================================================

# ================= USER INPUT =================
# Space-separated list; override via VARS="slope aspect" ./download_90m_through_aws.sh
VARS=${VARS:-"aspect-cosine aspect-sine aspect convergence cti dev-magnitude dev-scale dx dxx dxy dy dyy eastness elev-stdev geom northness pcurv rough-magnitude rough-scale roughness slope spi tcurv tpi tri vrm"}
LAT_MIN=38
LAT_MAX=48
LON_MIN=-82
LON_MAX=-68
OUTDIR="./90m"
# ==============================================

S3ROOT="s3://dataspace/OTDS.012020.4326.1/raster"
ENDPOINT="https://opentopography.s3.sdsc.edu"
TILE_SIZE=30

snap_down_to_tile() {
  # Floor to the nearest TILE_SIZE multiple (handles negatives correctly)
  local value=$1
  local size=$2
  if (( value >= 0 )); then
    echo $(( (value / size) * size ))
  else
    echo $(( ((value - (size - 1)) / size) * size ))
  fi
}

# ---- snap to 30° tile grid ----
lat_start=$(snap_down_to_tile "${LAT_MIN}" "${TILE_SIZE}")
lat_end=$(snap_down_to_tile "${LAT_MAX}" "${TILE_SIZE}")
lon_start=$(snap_down_to_tile "${LON_MIN}" "${TILE_SIZE}")
lon_end=$(snap_down_to_tile "${LON_MAX}" "${TILE_SIZE}")

download_var() {
  local VAR="$1"
  mkdir -p "${OUTDIR}/${VAR}"
  cd "${OUTDIR}/${VAR}"

  for lat in $(seq $lat_start 30 $lat_end); do
    if (( lat < 0 )); then
      lat_tag="s$(printf "%02d" $((-lat)))"
    else
      lat_tag="n$(printf "%02d" $lat)"
    fi

    for lon in $(seq $lon_start 30 $lon_end); do
      if (( lon < 0 )); then
        lon_tag="w$(printf "%03d" $((-lon)))"
      else
        lon_tag="e$(printf "%03d" $lon)"
      fi

      fname="${VAR}_90M_${lat_tag}${lon_tag}.tar.gz"
      s3path="${S3ROOT}/${VAR}/${fname}"

      # Download if missing
      if [[ -f "${fname}" ]]; then
        echo "✔ ${fname} exists — skipping"
      else
        echo "⬇ Downloading ${fname}"
        aws s3 cp \
          "${s3path}" \
          "${fname}" \
          --endpoint-url "${ENDPOINT}" \
          --no-sign-request || {
            echo "⚠ Missing tile: ${fname}"
            continue
          }
      fi

      # Extract only once
      tif_pattern="${VAR}_90M_${lat_tag}${lon_tag}_*.tif"
      if ls ${tif_pattern} >/dev/null 2>&1; then
        echo "✔ ${lat_tag}${lon_tag} already extracted"
      else
        echo "📦 Extracting ${fname}"
        tar -xzf "${fname}"
      fi
    done
  done
  cd - >/dev/null
}

for VAR in ${VARS}; do
  echo "=== Processing variable: ${VAR} ==="
  download_var "${VAR}"
done

echo "✅ Done."
