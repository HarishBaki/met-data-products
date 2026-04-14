#!/usr/bin/env bash
set -euo pipefail

# Loop over all available variables; override via VARS="slope aspect" ./run_all_vars.sh
VARS=${VARS:-"aspect-cosine aspect-sine aspect convergence cti dev-magnitude dev-scale dx dxx dxy dy dyy eastness elev-stdev geom northness pcurv rough-magnitude rough-scale roughness slope spi tcurv tpi tri vrm"}

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
ROOT_DIR="/network/rit/lab/basulab/Harish/DFS"
VRT_DIR="${ROOT_DIR}/Geomorpho90m_downloaders/vrt_files"

cd "${ROOT_DIR}"
shopt -s nullglob

for VAR in ${VARS}; do
  echo "=== ${VAR} ==="
  tif_glob=("/network/rit/lab/basulab/RAW_DATA/Geomorpho90m/90m/${VAR}/${VAR}_90M_"*.tif)
  if (( ${#tif_glob[@]} == 0 )); then
    echo "WARN: No TIFs found for ${VAR}; skipping."
    continue
  fi

  mkdir -p "${VRT_DIR}"
  vrt_path="${VRT_DIR}/${VAR}_90m.vrt"
  echo "Building VRT: ${vrt_path}"
  gdalbuildvrt -overwrite "${vrt_path}" "${tif_glob[@]}"

  echo "Processing ${VAR} with process_tiff.py"
  python "${SCRIPT_DIR}/process_tiff.py" "${VAR}" --vrt "${vrt_path}"
done
