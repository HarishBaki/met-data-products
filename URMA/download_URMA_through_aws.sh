#!/usr/bin/env bash
set -euo pipefail
data_type='urma' # 'rtma' or 'urma', only need modification here for rtma or urma
data_category=$data_type"2p5" # 'rtma2p5' or 'urma2p5'
OUT="/network/rit/lab/basulab/RAW_DATA/${data_type^^}"

START="2025-12-01"  #"2014-01-28"
END="2025-12-31"

d="$START"
while [ "$(date -u -d "$d" +%s)" -le "$(date -u -d "$END" +%s)" ]; do
  ymd="$(date -u -d "$d" +%Y%m%d)"
  echo "==> ${ymd}"

  aws s3 sync --no-sign-request \
  s3://noaa-$data_type-pds/$data_category.${ymd}/ "${OUT}/${ymd}/" \
  --exclude "*" \
  --include "urma2p5*pcp_*grb2*" \
  --include "urma2p5*2dvaranl*grb2*" \
  --exclude "*mask*" \
  --exclude "*2dvarerr*" \
  --exclude "*2dvarges*" \
  --exclude "*.idx" \
  --only-show-errors

  d="$(date -I -u -d "$d + 1 day")"
done
