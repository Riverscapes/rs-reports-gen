#!/bin/bash
# Set -e will cause the script to exit if any command fails
# Set -u will cause the script to exit if any variable is not set
set -eu
# Set IFS to newline and tab to prevent word splitting issues and improve script safety
IFS=$'\n\t'

# Environment variables 
(: "${REPORT_ID?}")
(: "${USER_ID?}")
(: "${SPATIALITE_PATH?}")

# Turn off the set -u option once we've checked all the mandatory variables
set +u

cat<<EOF

 ▄▀▀▄▀▀▀▄  ▄▀▀▀▀▄      ▄▀▀▄▀▀▀▄  ▄▀▀█▄▄▄▄  ▄▀▀▄▀▀▀▄      ▄▀▀▀▀▄   ▄▀▀█▄▄▄▄  ▄▀▀▄ ▀▄ 
█   █   █ █ █   ▐     █   █   █ ▐  ▄▀   ▐ █   █   █     █        ▐  ▄▀   ▐ █  █ █ █ 
▐  █▀▀█▀     ▀▄       ▐  █▀▀█▀    █▄▄▄▄▄  ▐  █▀▀▀▀      █    ▀▄▄   █▄▄▄▄▄  ▐  █  ▀█ 
 ▄▀    █  ▀▄   █       ▄▀    █    █    ▌     █          █     █ █  █    ▌    █   █  
█     █    █▀▀▀       █     █    ▄▀▄▄▄▄    ▄▀           ▐▀▄▄▄▄▀ ▐ ▄▀▄▄▄▄   ▄▀   █   
▐     ▐    ▐          ▐     ▐    █    ▐   █             ▐         █    ▐   █    ▐   
                                 ▐        ▐                       ▐        ▐        
                                                   
EOF

echo "REPORT_ID: $REPORT_ID"
echo "USER_ID: $USER_ID"
echo "SPATIALITE_PATH: $SPATIALITE_PATH"


echo "======================  Initial Disk space usage ======================="
df -h

try() {

  # Activate venv (assumes it was created in Docker build or earlier)
  uv sync
  source /app/.venv/bin/activate 

  # TODO: download inputs 

  rpt-igo-project \
     $SPATIALITE_PATH \
     "{env:DATA_ROOT}/athena_to_rme2" \
     "/mnt/c/nardata/work/rme_extraction/20250827-rkymtn/physio_rky_mtn_system_4326.geojson" \
     "Physio Rky Mtn System"

  # Future Enhancement - upload incomplete reports to a debug service instead
  if [[ $? != 0 ]]; then return 1; fi

  echo "======================  Final Disk space usage ======================="
  df -h

  echo "======================  Upload to the report server (S3) ======================"
  # TODO: upload outputs

}