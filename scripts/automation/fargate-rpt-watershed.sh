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
                                                              d8b                   d8b 
                                d8P                           ?88                   88P 
                             d888888P                          88b                 d88  
     ?88   d8P  d8P d888b8b    ?88'   d8888b  88bd88b .d888b,  888888b  d8888b d888888  
     d88  d8P' d8P'd8P' ?88    88P   d8b_,dP  88P'  ` ?8b,     88P `?8bd8b_,dPd8P' ?88  
     ?8b ,88b ,88' 88b  ,88b   88b   88b     d88        `?8b  d88   88P88b    88b  ,88b 
     `?888P'888P'  `?88P'`88b  `?8b  `?888P'd88'     `?888P' d88'   88b`?888P'`?88P'`88b

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

  HUCID=$(python3 -c "import json,sys; print(json.load(sys.stdin).get('properties').get('id',None))" < "$INPUTS_DIR/input.geojson")
  echo "======================  Running rpt-watershed-summary ======================="
  python -m reports.rpt_watershed_summary.main \
    "$OUTPUTS_DIR" \
    "$HUCID" \
    "$REPORT_NAME" \
    --include_pdf \
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
