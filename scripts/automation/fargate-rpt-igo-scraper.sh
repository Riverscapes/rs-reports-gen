#!/bin/bash
# Abort on errors (-e) and unset variables (-u)
set -eu
IFS=$'\n\t'

# Required environment variables
(: "${REPORT_ID?}")
(: "${USER_ID?}")
(: "${API_TOKEN?}")

STAGE=${STAGE:-STAGING}


cat <<'EOF'
 ▄▀▀█▀▄    ▄▀▀▀▀▄    ▄▀▀▀▀▄                                     
█   █  █  █         █      █                                    
▐   █  ▐  █    ▀▄▄  █      █                                    
    █     █     █ █ ▀▄    ▄▀                                    
 ▄▀▀▀▀▀▄  ▐▀▄▄▄▄▀ ▐   ▀▀▀▀                                      
█       █ ▐                                                     
▐       ▐                                                       
 ▄▀▀▀▀▄  ▄▀▄▄▄▄   ▄▀▀▄▀▀▀▄  ▄▀▀█▄   ▄▀▀▄▀▀▀▄  ▄▀▀█▄▄▄▄  ▄▀▀▄▀▀▀▄
█ █   ▐ █ █    ▌ █   █   █ ▐ ▄▀ ▀▄ █   █   █ ▐  ▄▀   ▐ █   █   █
   ▀▄   ▐ █      ▐  █▀▀█▀    █▄▄▄█ ▐  █▀▀▀▀    █▄▄▄▄▄  ▐  █▀▀█▀ 
▀▄   █    █       ▄▀    █   ▄▀   █    █        █    ▌   ▄▀    █ 
 █▀▀▀    ▄▀▄▄▄▄▀ █     █   █   ▄▀   ▄▀        ▄▀▄▄▄▄   █     █  
 ▐      █     ▐  ▐     ▐   ▐   ▐   █          █    ▐   ▐     ▐  
        ▐                          ▐          ▐                                                                
EOF

echo "REPORT_ID: $REPORT_ID"
echo "USER_ID: $USER_ID"
echo "RSReports STAGE: $STAGE"


echo "======================  Initial Disk space usage ======================="
df -h

WORK_ROOT="/usr/local/data"
INPUTS_DIR="$WORK_ROOT/inputs"
OUTPUTS_DIR="$WORK_ROOT/output"
PYTHONPATH="/usr/local/rs-reports-gen/src:${PYTHONPATH:-}"

uv sync
source /usr/local/rs-reports-gen/.venv/bin/activate

try() {
  echo "======================  Downloading inputs ======================="
  python -m api.downloadInputs \
    "$INPUTS_DIR" \
    --user-id "$USER_ID" \
    --report-id "$REPORT_ID" \
    --stage "$STAGE"
  if [[ $? != 0 ]]; then return 1; fi

  # Extract the "name" property from the $INPUTS_DIR/inputs/index.json file
  REPORT_NAME=$(python3 -c "import json,sys; print(json.load(sys.stdin).get('name',None))" < "$INPUTS_DIR/index.json")
  if [[ -z "$REPORT_NAME" || "$REPORT_NAME" == "null" ]]; then
    echo "Error: Report name not found in $INPUTS_DIR/index.json"
    return 1
  fi

  echo "======================  Running rpt_igo_project ======================="
  python -m reports.rpt_igo_project.main \
    "/usr/local/lib/mod_spatialite.so" \
    "$OUTPUTS_DIR" \
    "$INPUTS_DIR/input.geojson" \
    "$REPORT_NAME"
  if [[ $? != 0 ]]; then return 1; fi

  echo "======================  Uploading outputs ======================="
  python -m api.uploadOutputs \
    "$OUTPUTS_DIR" \
    --user-id "$USER_ID" \
    --report-id "$REPORT_ID" \
    --stage "$STAGE"
  if [[ $? != 0 ]]; then return 1; fi

  echo "======================  Final Disk space usage ======================="
  df -h

  # Cleanup
  echo "<<PROCESS COMPLETE>>\n\n"

}
try || {
  # Emergency Cleanup
  echo "<<ENDED WITH AN ERROR>>\n\n"
  exit 1
}
