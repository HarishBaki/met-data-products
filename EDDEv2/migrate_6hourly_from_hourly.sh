#!/usr/bin/env bash
# One-time migration: some 6-hourly EDDEv2 variables (psl, zg500, ...) were
# downloaded into RAW_DATA/EDDE_V2/hourly/ instead of RAW_DATA/EDDE_V2/6hourly/
# (the old mv_files.sh migration hardcoded a single "hourly" destination for
# every variable, with no cadence awareness). This moves them to where they
# actually belong, matching the S3 bucket's own hourly/ vs 6hourly/ split.
#
# Selection is by the file's own freqtag ("*.6hr.*.raw.nc"), not a hardcoded
# variable list - this catches any 6-hourly variable currently misplaced
# under hourly/, not just the two known today, and will not touch genuine
# 1-hourly files (pr, ps, ts, ua, va, ...) which never carry ".6hr." in
# their name.
#
# Safe to re-run: for each file, if the destination already exists with the
# same size, it is treated as already migrated and skipped. If the
# destination exists with a DIFFERENT size, the script aborts immediately
# without touching that file - that state means something unexpected
# happened and wants a human look, not an automated overwrite.
#
# Defaults to a dry run (prints every planned move, touches nothing).
# Pass --execute to actually perform the moves.
#
# Usage:
#   ./migrate_6hourly_from_hourly.sh            # dry run
#   ./migrate_6hourly_from_hourly.sh --execute   # actually move files

set -euo pipefail

SRC_ROOT="/network/rit/lab/basulab/RAW_DATA/EDDE_V2/hourly"
DEST_ROOT="/network/rit/lab/basulab/RAW_DATA/EDDE_V2/6hourly"

EXECUTE=0
if [[ "${1:-}" == "--execute" ]]; then
  EXECUTE=1
fi

if [[ ! -d "${SRC_ROOT}" ]]; then
  echo "ERROR: source root does not exist: ${SRC_ROOT}"
  exit 1
fi

n_planned=0
n_moved=0
n_already_ok=0
n_aborted=0

# NUL-delimited to survive filenames with spaces (none expected here, but
# don't rely on that).
while IFS= read -r -d '' src; do
  rel="${src#${SRC_ROOT}/}"
  dest="${DEST_ROOT}/${rel}"
  dest_dir="$(dirname "${dest}")"

  n_planned=$((n_planned + 1))

  if [[ -e "${dest}" ]]; then
    src_size="$(stat -c%s "${src}")"
    dest_size="$(stat -c%s "${dest}")"
    if [[ "${src_size}" == "${dest_size}" ]]; then
      n_already_ok=$((n_already_ok + 1))
      continue
    else
      echo "ABORT: size mismatch, refusing to touch either file:"
      echo "  src  (${src_size} bytes): ${src}"
      echo "  dest (${dest_size} bytes): ${dest}"
      echo "This needs a human look before continuing. Stopping now -" \
           "no further files will be moved this run."
      n_aborted=$((n_aborted + 1))
      break
    fi
  fi

  if [[ "${EXECUTE}" -eq 1 ]]; then
    mkdir -p "${dest_dir}"
    mv "${src}" "${dest}"
    echo "Moved: ${rel}"
    n_moved=$((n_moved + 1))
  else
    echo "Would move: ${rel}"
  fi
done < <(find "${SRC_ROOT}" -type f -name "*.6hr.*.raw.nc" -print0 | sort -z)

echo
echo "=========================================="
if [[ "${EXECUTE}" -eq 1 ]]; then
  echo "Moved:              ${n_moved}"
else
  echo "Would move:         ${n_planned}"
fi
echo "Already at dest:    ${n_already_ok}"
echo "Aborted (mismatch): ${n_aborted}"
echo "=========================================="

if [[ "${n_aborted}" -gt 0 ]]; then
  exit 2
fi

if [[ "${EXECUTE}" -eq 0 ]]; then
  echo
  echo "This was a dry run. Nothing was moved. Re-run with --execute to perform it."
fi
