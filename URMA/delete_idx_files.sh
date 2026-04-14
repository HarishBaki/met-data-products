#!/usr/bin/env bash
set -euo pipefail

# Mirror the downloader date loop and delete .idx files in OUT/ymd folders.
data_type='urma' # adjust if needed
OUT="/network/rit/lab/basulab/RAW_DATA/${data_type^^}"

START="2014-01-28"
END="2025-12-31"

d="$START"
while [ "$(date -u -d "$d" +%s)" -le "$(date -u -d "$END" +%s)" ]; do
  ymd="$(date -u -d "$d" +%Y%m%d)"
  target_dir="${OUT}/${ymd}"

  if [[ -d "$target_dir" ]]; then
    echo "==> ${ymd} : deleting *.idx in ${target_dir}"
    find "$target_dir" -type f -name "*.idx" -print -delete
  else
    echo "==> ${ymd} : skip (missing ${target_dir})"
  fi

  d="$(date -I -u -d "$d + 1 day")"
done
