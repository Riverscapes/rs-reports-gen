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
 ____  ____  ____    _____            _                            
|  _ \| __ )|  _ \  | ____|_  ___ __ | | ___  _ __ ___ _ __        
| |_) |  _ \| |_) | |  _| \ \/ / '_ \| |/ _ \| '__/ _ \ '__|       
|  __/| |_) |  _ <  | |___ >  <| |_) | | (_) | | |  __/ |          
|_|   |____/|_| \_\ |_____/_/\_\ .__/|_|\___/|_|  \___|_|          
                               |_|                                  
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

  REPORT_NAME=$(python3 -c "import json,sys; print(json.load(sys.stdin).get('name',None))" < "$INPUTS_DIR/index.json")
  if [[ -z "$REPORT_NAME" || "$REPORT_NAME" == "null" ]]; then
    echo "Error: Report name not found in $INPUTS_DIR/index.json"
    return 1
  fi

  echo "======================  Running rpt-pbr-explorer ======================="
  python -m reports.rpt_pbr_explorer.main \
    "$OUTPUTS_DIR" \
    "$INPUTS_DIR/input.geojson" \
    "$REPORT_NAME" \
    --include_pdf \
    --unit_system "$UNIT_SYSTEM"
  if [[ $? != 0 ]]; then return 1; fi

  echo "======================  Zipping up ======================="
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

  echo "<<PROCESS COMPLETE>>\n\n"
}

try || {
  python -m api.uploadOutputs \
    "$OUTPUTS_DIR" \
    --user-id "$USER_ID" \
    --report-id "$REPORT_ID" \
    --stage "$STAGE" \
    --log-only
  echo "<<REPORT PROCESS ENDED WITH AN ERROR>>\n\n"
  exit 1
}
