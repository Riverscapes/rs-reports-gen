#!/bin/bash
# Abort on error (-e) and unset variables (-u)
set -eu
# Restrict word splitting to safe characters
IFS=$'\n\t'

# Mandatory environment variables
(: "${DATA_ROOT?}")
(: "${REPORT_ID?}")
(: "${USER_ID?}")
(: "${API_TOKEN?}")
(: "${SPATIALITE_PATH?}")
(: "${AOI_GEOJSON_PATH?}")
(: "${PROJECT_NAME?}")

# Optional stage defaults to STAGING
STAGE=${STAGE:-STAGING}
STAGE=${STAGE^^}

# Allow optional vars below to be empty without exiting
set +u

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

set -u

echo "REPORT_ID: $REPORT_ID"
echo "USER_ID: $USER_ID"
echo "SPATIALITE_PATH: $SPATIALITE_PATH"
echo "STAGE: $STAGE"

if [[ ! -f "$SPATIALITE_PATH" ]]; then
  echo "mod_spatialite not found: $SPATIALITE_PATH" >&2
  exit 1
fi

if [[ ! -f "$AOI_GEOJSON_PATH" ]]; then
  echo "AOI file not found: $AOI_GEOJSON_PATH" >&2
  exit 1
fi

echo "======================  Initial Disk space usage ======================="
df -h

# Derived paths
WORK_ROOT="${DATA_ROOT%/}/rpt-igo-project"
INPUTS_DIR="$WORK_ROOT/inputs"
WORKING_FOLDER="$WORK_ROOT/work"
PROJECT_DIR="$WORKING_FOLDER/project"

mkdir -p "$INPUTS_DIR" "$WORKING_FOLDER"

# Ensure dependencies are present and virtualenv is active
uv sync
source /app/.venv/bin/activate
if [[ -d /app/src ]]; then
  export PYTHONPATH="/app/src:${PYTHONPATH:-}"
fi

python -m api.downloadInputs \
  "$INPUTS_DIR" \
  --api-key "$API_TOKEN" \
  --user-id "$USER_ID" \
  --report-id "$REPORT_ID" \
  --stage "$STAGE"

echo "======================  Running rpt-igo-project ======================="
python -m reports.rpt_igo_project.main \
  "$SPATIALITE_PATH" \
  "$WORKING_FOLDER" \
  "$AOI_GEOJSON_PATH" \
  "$PROJECT_NAME"

echo "======================  Final Disk space usage ======================="
df -h

echo "======================  Uploading outputs ======================="
if [[ -d "$PROJECT_DIR" ]]; then
  python -m api.uploadOutputs \
    "$PROJECT_DIR" \
    --api-key "$API_TOKEN" \
    --user-id "$USER_ID" \
    --report-id "$REPORT_ID" \
    --stage "$STAGE" \
    --file-type OUTPUTS
else
  echo "Project directory not found: $PROJECT_DIR" >&2
  exit 1
fi

