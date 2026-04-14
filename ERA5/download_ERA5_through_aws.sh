#!/usr/bin/env bash
set -euo pipefail

# Download ERA5 surface analysis and forecast files needed to derive:
# sp, 10u, 10v, 2t, 2d (analysis)
# lsp, cp (for tp = lsp + cp)
# i10fg (gusts)
# Time range: 2018–2025 (filtered via include/exclude)
# Destination structure mirrors bucket paths under DEST.

DEST="/network/rit/lab/basulab/RAW_DATA/ERA5"
BUCKET="s3://nsf-ncar-era5"
START_YM="201712"
END_YM="202512"

mkdir -p "${DEST}"

YM="${START_YM}"
while [[ "${YM}" -le "${END_YM}" ]]; do
  echo "=== Year-Month ${YM} ==="

  # Analysis vars
  for code in 128_134_sp 128_165_10u 128_166_10v 128_167_2t 128_168_2d; do
    src="s3://${BUCKET#s3://}/e5.oper.an.sfc/${YM}/e5.oper.an.sfc.${code}.ll025sc.${YM}*.nc"
    dest="${DEST}/e5.oper.an.sfc/${YM}/"
    mkdir -p "${dest}"
    s5cmd --no-sign-request --numworkers 32 cp "${src}" "${dest}" || true
  done

  # Forecast accum (lsp, cp)
  for code in 128_142_lsp 128_143_cp; do
    src="s3://${BUCKET#s3://}/e5.oper.fc.sfc.accumu/${YM}/e5.oper.fc.sfc.accumu.${code}.ll025sc.${YM}*.nc"
    dest="${DEST}/e5.oper.fc.sfc.accumu/${YM}/"
    mkdir -p "${dest}"
    s5cmd --no-sign-request --numworkers 32 cp "${src}" "${dest}" || true
  done

  # Forecast instantaneous gust (i10fg)
  src="s3://${BUCKET#s3://}/e5.oper.fc.sfc.instan/${YM}/e5.oper.fc.sfc.instan.228_029_i10fg.ll025sc.${YM}*.nc"
  dest="${DEST}/e5.oper.fc.sfc.instan/${YM}/"
  mkdir -p "${dest}"
  s5cmd --no-sign-request --numworkers 32 cp "${src}" "${dest}" || true

  YM=$(date -d "${YM:0:4}-${YM:4:2}-01 +1 month" +"%Y%m")
done
