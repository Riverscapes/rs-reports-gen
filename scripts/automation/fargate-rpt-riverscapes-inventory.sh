#!/bin/bash
# Abort on errors (-e) and unset variables (-u)
set -eu
IFS=$'\n\t'

# Required environment variables
(: "${REPORT_ID?}")
(: "${USER_ID?}")
(: "${API_TOKEN?}")
(: "${UNIT_SYSTEM?}")
NO_UI=true

STAGE=${STAGE:-STAGING}

cat <<'EOF'
ooooooooo.    o8o                                                                                                ooooo                                                   .                                  
`888   `Y88.  `"'                                                                                                `888'                                                 .o8                                  
 888   .d88' oooo  oooo    ooo  .ooooo.  oooo d8b  .oooo.o  .ooooo.   .oooo.   oo.ooooo.   .ooooo.   .oooo.o      888  ooo. .oo.   oooo    ooo  .ooooo.  ooo. .oo.   .o888oo  .ooooo.  oooo d8b oooo    ooo 
 888ooo88P'  `888   `88.  .8'  d88' `88b `888""8P d88(  "8 d88' `"Y8 `P  )88b   888' `88b d88' `88b d88(  "8      888  `888P"Y88b   `88.  .8'  d88' `88b `888P"Y88b    888   d88' `88b `888""8P  `88.  .8'  
 888`88b.     888    `88..8'   888ooo888  888     `"Y88b.  888        .oP"888   888   888 888ooo888 `"Y88b.       888   888   888    `88..8'   888ooo888  888   888    888   888   888  888       `88..8'   
 888  `88b.   888     `888'    888    .o  888     o.  )88b 888   .o8 d8(  888   888   888 888    .o o.  )88b      888   888   888     `888'    888    .o  888   888    888 . 888   888  888        `888'    
o888o  o888o o888o     `8'     `Y8bod8P' d888b    8""888P' `Y8bod8P' `Y888""8o  888bod8P' `Y8bod8P' 8""888P'     o888o o888o o888o     `8'     `Y8bod8P' o888o o888o   "888" `Y8bod8P' d888b        .8'     
                                                                                888                                                                                                             .o..P'      
                                                                               o888o                                                                                                            `Y8P'       
                                                                                                                                                                                                                                                                                                                                                                                                                       
EOF

echo "REPORT_ID: $REPORT_ID"
echo "USER_ID: $USER_ID"
echo "RSReports STAGE: $STAGE"
echo "Unit System: $UNIT_SYSTEM"


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

  echo "======================  Running rpt-riverscapes-inventory ======================="
  python -m reports.rpt_riverscapes_inventory.main \
    "$OUTPUTS_DIR" \
    "$INPUTS_DIR/input.geojson" \
    "$REPORT_NAME" \
    --unit_system "$UNIT_SYSTEM"
  if [[ $? != 0 ]]; then return 1; fi

  echo "======================  Zipping up ======================="
  # Add everything in the outputs directory to a zip file inside the outputs directory
  (cd "$OUTPUTS_DIR" && zip -r "report.zip" . --exclude "report.zip")
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
  # On error, upload logs only
  python -m api.uploadOutputs \
    "$OUTPUTS_DIR" \
    --user-id "$USER_ID" \
    --report-id "$REPORT_ID" \
    --stage "$STAGE" \
    --log-only  
  # Emergency Cleanup
  echo "<<REPORT PROCESS ENDED WITH AN ERROR>>\n\n"
  exit 1
}
