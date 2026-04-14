#!/usr/bin/env bash
set -euo pipefail

prefix="${1:-s3://epa-edde-v2/EDDE_V2/hourly/WRF-MPI/SSP2-4.5/2025-2100/2025/}"
dest_dir="${2:-/network/rit/lab/basulab/RAW_DATA/EDDEv2_first_month_files}"

bucket_and_path="${prefix#s3://}"
bucket="${bucket_and_path%%/*}"
if [[ "${bucket}" == "${bucket_and_path}" ]]; then
  echo "Prefix must include bucket and path, e.g. s3://bucket/path/"
  exit 1
fi

mkdir -p "${dest_dir}"

tmp_list="$(mktemp)"
aws s3 ls --no-sign-request "${prefix}" --recursive | awk '{print $4}' | grep '\.nc$' | sort > "${tmp_list}"

declare -A seen
while IFS= read -r key; do
  file="$(basename "${key}")"
  var="${file%%.*}"
  if [[ -z "${seen[$var]+x}" ]]; then
    seen[$var]=1
    aws s3 cp --no-sign-request "s3://${bucket}/${key}" "${dest_dir}/"
  fi
done < "${tmp_list}"

rm -f "${tmp_list}"
