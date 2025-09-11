#!/bin/bash
# Set -e will cause the script to exit if any command fails
# Set -u will cause the script to exit if any variable is not set
set -eu

cat<<EOF

 ▄▀▀▄▀▀▀▄  ▄▀▀▀▀▄      ▄▀▀▄▀▀▀▄  ▄▀▀█▄▄▄▄  ▄▀▀▄▀▀▀▄      ▄▀▀▀▀▄   ▄▀▀█▄▄▄▄  ▄▀▀▄ ▀▄ 
█   █   █ █ █   ▐     █   █   █ ▐  ▄▀   ▐ █   █   █     █        ▐  ▄▀   ▐ █  █ █ █ 
▐  █▀▀█▀     ▀▄       ▐  █▀▀█▀    █▄▄▄▄▄  ▐  █▀▀▀▀      █    ▀▄▄   █▄▄▄▄▄  ▐  █  ▀█ 
 ▄▀    █  ▀▄   █       ▄▀    █    █    ▌     █          █     █ █  █    ▌    █   █  
█     █    █▀▀▀       █     █    ▄▀▄▄▄▄    ▄▀           ▐▀▄▄▄▄▀ ▐ ▄▀▄▄▄▄   ▄▀   █   
▐     ▐    ▐          ▐     ▐    █    ▐   █             ▐         █    ▐   █    ▐   
                                 ▐        ▐                       ▐        ▐        
                                                   
EOF

echo "======================  Initial Disk space usage ======================="
df -h

try() {

  # Activate venv (assumes it was created in Docker build or earlier)
  source /app/.venv/bin/activate 

  rpt-igo-project \
     "/usr/lib/x86_64-linux-gnu/mod_spatialite.so" \
     "{env:DATA_ROOT}/athena_to_rme2" \
     "/mnt/c/nardata/work/rme_extraction/20250827-rkymtn/physio_rky_mtn_system_4326.geojson" \
     "Physio Rky Mtn System"

  # python3 -m rpt-igo-project

}