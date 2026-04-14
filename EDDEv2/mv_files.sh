#!/usr/bin/env bash
set -euo pipefail

# Adjust these if needed
SRC_ROOT="/network/rit/lab/basulab/RAW_DATA/EDDEv2"
DEST_ROOT="/network/rit/lab/basulab/RAW_DATA/EDDE_V2/hourly/"
VAR_DIRS=(
  "EDDEv2_PCP"
  "EDDEv2_PSL"
  "EDDEv2_RH"
  "EDDEv2_TS"
  "EDDEv2_WDIR"
  "EDDEv2_WSPDS"
  "EDDEv2_ZG500"
)

for var_dir in "${VAR_DIRS[@]}"; do
  src_base="${SRC_ROOT}/${var_dir}"

  # Find files under each variable root and recreate relative structure under DEST_ROOT
  while IFS= read -r -d '' f; do
    rel="${f#${src_base}/}"             # strip variable prefix
    dest="${DEST_ROOT}/${rel}"
    dest_dir="$(dirname "${dest}")"
    mkdir -p "${dest_dir}"

    # move only if destination doesn't exist
    if [[ ! -e "${dest}" ]]; then
      echo "Moving: ${f} -> ${dest}"
      mv "${f}" "${dest}"
    fi
  done < <(find "${src_base}" -type f -print0)
done
