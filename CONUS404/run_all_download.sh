#!/bin/bash

# Loop through years 1979 to 2022
for YEAR in $(seq 1979 2022); do

  # figure out number of days in the year
  if date -d "$YEAR-02-29" >/dev/null 2>&1; then
    DAYS=366
  else
    DAYS=365
  fi

  # special case for 1979 (starts on Oct 1)
  if [ "$YEAR" -eq 1979 ]; then
    START_DATE="1979-10-01"
    DAYS=92
  else
    START_DATE="${YEAR}-01-01"
  fi

  echo "Submitting array job for YEAR=$YEAR with $DAYS days (start=$START_DATE)"

  # submit job array, passing YEAR and START_DATE to the job script
  sbatch --array=0-$((DAYS-1)) jobsub_conus_download.slurm $START_DATE array

done