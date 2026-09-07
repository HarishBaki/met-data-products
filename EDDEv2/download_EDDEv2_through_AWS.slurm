#!/bin/bash

#SBATCH --job-name=download_EDDEv2
#SBATCH --output=slurmout/download_EDDEv2-%j.out
#SBATCH --error=slurmout/download_EDDEv2-%j.err
#SBATCH --time=05-00:00:00
#SBATCH --nodes=1
#SBATCH --ntasks-per-node=1
#SBATCH --cpus-per-task=16
#SBATCH --propagate=NONE

# Ensure log directory exists
mkdir -p slurmout

# Load conda init script
source /network/rit/lab/basulab/mambaforge/etc/profile.d/conda.sh
conda activate DFSAI

echo "Job start: $(date)"

# Usage:
#   VAR_NAME=<var> START_YEAR=<y0> END_YEAR=<y1> ./download_EDDEv2_through_AWS.slurm
#
# The S3 prefix and local dest_root are always derived from VAR_NAME via
# catalog_with_vars.csv (see lookup_frequency.py) - there is no hardcoded
# frequency default anywhere in this script. If VAR_NAME resolves to more
# than one frequency (a real, rare case - e.g. "pr" exists at both hourly
# and hifr with different meaning), also set FREQUENCY to disambiguate;
# lookup_frequency.py validates it against the catalog rather than trusting
# it blindly, so a typo there still fails loudly instead of silently
# fetching the wrong product.
#
# Examples:
#   VAR_NAME=td START_YEAR=2025 END_YEAR=2030 NUM_WORKERS=16 \
#     ./download_EDDEv2_through_AWS.slurm
#   VAR_NAME=pr FREQUENCY=hifr START_YEAR=2025 END_YEAR=2030 \
#     ./download_EDDEv2_through_AWS.slurm

: "${VAR_NAME:?VAR_NAME must be set (e.g. VAR_NAME=td) -- no default, so a variable can never silently resolve to the wrong frequency folder}"
var_name="${VAR_NAME}"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
lookup_args=("${var_name}")
if [[ -n "${FREQUENCY:-}" ]]; then
  lookup_args+=(--frequency "${FREQUENCY}")
fi
frequency="$(python "${SCRIPT_DIR}/lookup_frequency.py" "${lookup_args[@]}")" || exit 1

prefix="s3://epa-edde-v2/EDDE_V2/${frequency}"
dest_root="/network/rit/lab/basulab/RAW_DATA/EDDE_V2/${frequency}"
num_workers="${NUM_WORKERS:-${SLURM_CPUS_PER_TASK:-16}}"
start_year="${START_YEAR:-2031}"
end_year="${END_YEAR:-2100}"

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

echo "Config: var_name=${var_name} frequency=${frequency} prefix=${prefix} dest_root=${dest_root} years=${start_year}-${end_year} num_workers=${num_workers}"
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
