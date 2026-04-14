#!/usr/bin/env bash
set -euo pipefail

# ==============================
# CONFIGURATION
# ==============================
MAX_PARALLEL=5
SLEEP_SEC=30
JOB_NAME="download_icon_dream"
SLURM_SCRIPT="download_icon_dream.slurm"

# Variable list (edit as needed)
ALL_VARS=(
  ASWDIFD_S  # Surface down solar diffuse radiation
  ASWDIR_S   # Surface down solar direct radiation
  CLCT       # Total cloud cover
  DEN        # Density of moist air
  P          # Pressure (model levels)
  PMSL       # Pressure reduced to mean sea level
  PS         # Surface pressure (not reduced)
  QV         # Specific humidity (model levels)
  QV_S       # Surface specific humidity
  T          # Temperature (model levels)
  TD_2M      # Dew point (2 m)
  TKE        # Turbulent kinetic energy
  TMAX_2M    # 2 m maximum temperature
  TMIN_2M    # 2 m minimum temperature
  TOT_PREC   # Total precipitation
  T_2M       # 2 m temperature
  U          # Zonal wind speed (model levels)
  U_10M      # 10 m zonal wind speed
  V          # Meridional wind speed (model levels)
  VMAX_10M   # Maximum wind (10 m)
  V_10M      # 10 m meridional wind speed
  WS         # Wind speed (model levels)
  WS_10M     # 10 m wind speed
  Z0         # Surface roughness
)
VARS=(
  WS
)

# Year range
START_YEARMM=201001
END_YEARMM=202512

# ==============================
# MAIN LOOP
# ==============================
for VAR in "${VARS[@]}"; do
    # Throttle submissions if too many active jobs
    while [ "$(squeue -u "$USER" -n "$JOB_NAME" -h | wc -l)" -ge "$MAX_PARALLEL" ]; do
      echo "Reached $MAX_PARALLEL jobs running. Waiting..."
      sleep "$SLEEP_SEC"
    done

    echo "Submitting: $VAR  $START_YEARMM to $END_YEARMM"
    sbatch "$SLURM_SCRIPT" "$VAR" "$START_YEARMM" "$END_YEARMM"

    sleep 1
done

echo "=============================================="
echo "All jobs submitted!"
echo "=============================================="
