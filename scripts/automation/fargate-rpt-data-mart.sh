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

# Optional environment variables
STAGE=${STAGE:-STAGING}
GENERATE_PBI=${GENERATE_PBI:-false}
INCLUDE_GEOMETRY=${INCLUDE_GEOMETRY:-false}

cat <<'EOF'
 ____        _          __  __            _   
|  _ \  __ _| |_ __ _  |  \/  | __ _ _ __| |_ 
| | | |/ _` | __/ _` | | |\/| |/ _` | '__| __|
| |_| | (_| | || (_| | | |  | | (_| | |  | |_ 
|____/ \__,_|\__\__,_| |_|  |_|\__,_|_|   \__|
EOF

echo "REPORT_ID: $REPORT_ID"
echo "USER_ID: $USER_ID"
echo "RSReports STAGE: $STAGE"
echo "Unit System: $UNIT_SYSTEM"
echo "GENERATE_PBI: $GENERATE_PBI"
echo "INCLUDE_GEOMETRY: $INCLUDE_GEOMETRY"

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

  # Extract the "name" property from the $INPUTS_DIR/index.json file
  REPORT_NAME=$(python3 -c "import json,sys; print(json.load(sys.stdin).get('name',None))" < "$INPUTS_DIR/index.json")
  if [[ -z "$REPORT_NAME" || "$REPORT_NAME" == "null" ]]; then
    echo "Error: Report name not found in $INPUTS_DIR/index.json"
    return 1
  fi

  echo "======================  Running rpt-data-mart ======================="
  CMD=(
    python -m reports.rpt_data_mart.main
    "$OUTPUTS_DIR"
    "$INPUTS_DIR/input.geojson"
    "$REPORT_NAME"
    --unit_system "$UNIT_SYSTEM"
  )


  if [[ "$GENERATE_PBI" == "true" ]]; then
    CMD+=(--generate-pbi)
  fi

  if [[ "$INCLUDE_GEOMETRY" == "true" ]]; then
    CMD+=(--include-geometry)
  fi

  "${CMD[@]}"
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
