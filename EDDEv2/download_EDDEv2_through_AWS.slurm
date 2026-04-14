#!/bin/bash

#SBATCH --job-name=download_EDDEv2
#SBATCH --output=slurmout/download_EDDEv2-%j.out
#SBATCH --error=slurmout/download_EDDEv2-%j.err
#SBATCH --time=05-00:00:00
#SBATCH --nodes=1
#SBATCH --ntasks-per-node=1
#SBATCH --cpus-per-task=32

# Ensure log directory exists
mkdir -p slurmout

# Load conda init script
source /network/rit/lab/basulab/mambaforge/etc/profile.d/conda.sh
conda activate DFSAI

echo "Job start: $(date)"

# Usage:
#   ./download_EDDEv2_through_AWS.slurm [s3_prefix] [dest_root]
#
# Examples:
#   VAR_NAME=td START_YEAR=2025 END_YEAR=2030 NUM_WORKERS=16 \
#     ./download_EDDEv2_through_AWS.slurm \
#     s3://epa-edde-v2/EDDE_V2/hourly \
#     /network/rit/lab/basulab/RAW_DATA/EDDE_V2/hourly
# var names can be from = ["clt", "hfls", "hfss", "hur", "hus", "pr", "ps", "rlds", "rlut", "rsds", "td", "ts", "ua", "ustar", "va", "wdirs", "wspds"]

prefix="${1:-s3://epa-edde-v2/EDDE_V2/hourly}"
dest_root="${2:-/network/rit/lab/basulab/RAW_DATA/EDDE_V2/hourly}"
num_workers="${NUM_WORKERS:-${SLURM_CPUS_PER_TASK:-16}}"
start_year="${START_YEAR:-1985}"
end_year="${END_YEAR:-2030}"
var_name="${VAR_NAME:-va}"

pattern="*${var_name}.*.nc"

tmp_list="$(mktemp)"
tmp_cmds="$(mktemp)"

# Build a regex for the year range.
years=()
for y in $(seq "${start_year}" "${end_year}"); do
  years+=("${y}")
done
years_regex="$(IFS='|'; echo "${years[*]}")"

# List matching files, then filter to the year range by path segment.
s5cmd --no-sign-request --numworkers "${num_workers}" ls \
  "${prefix}/**/${pattern}" | awk '{print $NF}' \
  | grep -E "/(${years_regex})/" > "${tmp_list}"

echo "Config: prefix=${prefix} dest_root=${dest_root} var_name=${var_name} years=${start_year}-${end_year} num_workers=${num_workers}"
file_count="$(wc -l < "${tmp_list}")"
echo "Files matched: ${file_count}"
if [[ "${file_count}" -eq 0 ]]; then
  echo "No files matched. Exiting."
  rm -f "${tmp_list}"
  exit 1
fi

index=0
while IFS= read -r key; do
  if [[ "${key}" != s3://* ]]; then
    key="${prefix}/${key}"
  fi
  rel="${key#${prefix}/}"
  dest="${dest_root}/${rel}"
  dest_dir="$(dirname "${dest}")"
  mkdir -p "${dest_dir}"
  index=$((index + 1))
  if (( index == 1 || index % 100 == 0 )); then
    echo "Downloading ${index}/${file_count}: ${key}"
  fi
  printf 'cp --no-clobber "%s" "%s"\n' "${key}" "${dest}" >> "${tmp_cmds}"
done < "${tmp_list}"

s5cmd --no-sign-request --numworkers "${num_workers}" run "${tmp_cmds}"

rm -f "${tmp_list}"
rm -f "${tmp_cmds}"
echo "Job end: $(date)"
