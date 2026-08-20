#!/bin/bash
# Abort on errors (-e) and unset variables (-u)
set -eu
IFS=$'\n\t'

# Required environment variables
(: "${REPORT_ID?}")
(: "${USER_ID?}")
(: "${API_TOKEN?}")

STAGE=${STAGE:-STAGING}
NO_UI=true

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
PROJECT_DIR="$OUTPUTS_DIR/project"
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
  # IGO script writes a complete Riverscapes project bundle. We stage that bundle in
  # a subfolder so we can publish selected user-facing artifacts separately while
  # keeping heavyweight project internals zip-only.
  python -m reports.rpt_igo_project.main \
    "/usr/local/lib/mod_spatialite.so" \
    "$PROJECT_DIR" \
    "$INPUTS_DIR/input.geojson" \
    "$REPORT_NAME" \
    --unit_system $UNIT_SYSTEM
  if [[ $? != 0 ]]; then return 1; fi

  echo "======================  Zipping up ======================="
  # Zip the full project as the canonical deliverable.
  (cd "$PROJECT_DIR" && zip -r "../report.zip" .)
  if [[ $? != 0 ]]; then return 1; fi

  echo "======================  Promoting standalone artifacts ======================="
  # Upload these top-level files separately for direct HTTP access in the frontend.
  cp "$PROJECT_DIR/report.html" "$OUTPUTS_DIR/report.html"
  if [[ -f "$PROJECT_DIR/report.pdf" ]]; then cp "$PROJECT_DIR/report.pdf" "$OUTPUTS_DIR/report.pdf"; fi
  if [[ -f "$PROJECT_DIR/report.log" ]]; then cp "$PROJECT_DIR/report.log" "$OUTPUTS_DIR/report.log"; fi

  # Remove unzipped project contents to avoid uploading duplicate large artifacts
  # (notably GeoPackage files) both standalone and inside report.zip.
  rm -fr "$PROJECT_DIR"

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
  echo "<<REPORT ENDED WITH AN ERROR>>\n\n"
  exit 1
}
