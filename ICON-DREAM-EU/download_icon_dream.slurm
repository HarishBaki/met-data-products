#!/bin/bash

#SBATCH --job-name=download_icon_dream
#SBATCH --output=slurmout/download_icon_dream-%j.out
#SBATCH --error=slurmout/download_icon_dream-%j.err
#SBATCH --time=05-00:00:00
#SBATCH --nodes=1
#SBATCH --ntasks-per-node=1
#SBATCH --cpus-per-task=32

set -euo pipefail

# Ensure log directory exists
mkdir -p slurmout

usage() {
  cat <<'EOF'
Usage: sbatch download_icon_dream.slurm VAR START_YYYYMM END_YYYYMM [PARALLEL] [DEST]

Example:
  sbatch download_icon_dream.slurm T_2M 201001 201203 32 /path/to/output
EOF
}

if [ "${1:-}" = "-h" ] || [ "${1:-}" = "--help" ]; then
  usage
  exit 0
fi

VAR="${1:-WS_10M}"
START="${2:-202101}"
END="${3:-202112}"
PARALLEL="${4:-${SLURM_CPUS_PER_TASK:-32}}"
DEST_BASE="${5:-/network/rit/lab/basulab/RAW_DATA/ICON-DREAM-EU}"

if [ -z "$VAR" ] || [ -z "$START" ] || [ -z "$END" ]; then
  usage
  exit 1
fi

if ! [[ "$START" =~ ^[0-9]{6}$ && "$END" =~ ^[0-9]{6}$ ]]; then
  echo "START and END must be in YYYYMM format" >&2
  exit 1
fi

BASE="https://opendata.dwd.de/climate_environment/REA/ICON-DREAM-EU/hourly/${VAR}/"
DEST="${DEST_BASE}/${VAR}"
mkdir -p "$DEST"

next_month() {
  local y="${1:0:4}"
  local m="${1:4:2}"
  m=$((10#$m + 1))
  if [ "$m" -eq 13 ]; then
    y=$((y + 1))
    m=1
  fi
  printf "%04d%02d" "$y" "$m"
}

download_one() {
  local yyyymm="$1"
  local file="ICON-DREAM-EU_${yyyymm}_${VAR}_hourly.grb"
  local url="${BASE}${file}"
  local dest="${DEST}/${file}"

  if [ -f "$dest" ]; then
    local local_size
    local remote_size
    local_size=$(stat -c%s "$dest")
    remote_size=$(curl -sI "$url" | awk -F': ' 'tolower($1)=="content-length"{print $2}' | tr -d '\r')
    if [ -n "$remote_size" ] && [ "$local_size" -eq "$remote_size" ]; then
      echo "skip ${file}"
      return 0
    fi
  fi

  echo "download ${file}"
  curl -L --fail --retry 3 -o "${dest}.part" "$url"
  mv "${dest}.part" "$dest"
}

export VAR BASE DEST
export -f download_one

cur="$START"
months=()
while [ "$cur" -le "$END" ]; do
  months+=("$cur")
  cur="$(next_month "$cur")"
done

printf '%s\n' "${months[@]}" | xargs -I{} -P "$PARALLEL" bash -c 'download_one "$1"' _ {}
