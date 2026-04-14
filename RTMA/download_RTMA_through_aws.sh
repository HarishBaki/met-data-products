# # 2015
# aws s3 cp --no-sign-request s3://noaa-rtma-pds/ /data/RTMA/2015/ \
#   --recursive \
#   --exclude "*" \
#   --include "rtma2p5.2015*/rtma2p5.t??z.2dvaranl_ndfd.grb2_ext"

# # 2016
# aws s3 cp --no-sign-request s3://noaa-rtma-pds/ /data/RTMA/2016/ \
#   --recursive \
#   --exclude "*" \
#   --include "rtma2p5.2016*/rtma2p5.t??z.2dvaranl_ndfd.grb2_ext"

# # 2017
# aws s3 cp --no-sign-request s3://noaa-rtma-pds/ /data/RTMA/2017/ \
#   --recursive \
#   --exclude "*" \
#   --include "rtma2p5.2017*/rtma2p5.t??z.2dvaranl_ndfd.grb2_ext"

#!/usr/bin/env bash
set -euo pipefail

OUT="/home/harish/harish_NAS/data/RTMA/2015-2017"
START="2015-01-01"
END="2017-12-31"

d="$START"
while [ "$(date -u -d "$d" +%s)" -le "$(date -u -d "$END" +%s)" ]; do
  ymd="$(date -u -d "$d" +%Y%m%d)"
  echo "==> ${ymd}"
  aws s3 sync --no-sign-request \
    "s3://noaa-rtma-pds/rtma2p5.${ymd}/" \
    "${OUT}/${ymd}/" \
    --exclude "*" \
    --include "rtma2p5.t*z.2dvaranl_ndfd.grb2_ext" \
    --only-show-errors 
  d="$(date -I -u -d "$d + 1 day")"
done