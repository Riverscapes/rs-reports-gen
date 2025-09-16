#!/bin/bash
# Abort on errors (-e) and unset variables (-u)
set -eu
IFS=$'\n\t'

# Required environment variables
(: "${REPORT_ID?}")
(: "${USER_ID?}")
(: "${API_TOKEN?}")

STAGE=${STAGE:-STAGING}

# Put back optional vars
set +u

cat <<'EOF'
 _ __                      _ __               __,                 
( /  ) o                  ( /  )         /   (                    
 /--< ,  _  ,__  _   (     /  / _  _  __/     `.   ,_   __,  _, _ 
/   \_(_/ |/ (/_/ (_/_)_  /  (_(/_(/_(_/_   (___)_/|_)_(_/(_(__(/_
                                                  /|              
                                        
EOF

set -u

echo "REPORT_ID: $REPORT_ID"
echo "USER_ID: $USER_ID"
echo "STAGE: $STAGE"


echo "======================  Initial Disk space usage ======================="
df -h

WORK_ROOT="/usr/local/data"
INPUTS_DIR="$WORK_ROOT/inputs"
RUN_ROOT="$WORK_ROOT/work"
PYTHONPATH="/usr/local/rs-reports-gen/src:${PYTHONPATH:-}"

mkdir -p "$INPUTS_DIR" "$RUN_ROOT"

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

  echo "======================  Running rpt-rivers-need-space ======================="
  python -m reports.rpt_rivers_need_space.main \
    "$RUN_ROOT" \
    "$INPUTS_DIR/input.geojson" \
    "Report name here"
  if [[ $? != 0 ]]; then return 1; fi

  echo "======================  Uploading outputs ======================="
  python -m api.uploadOutputs \
    "$LATEST_REPORT_DIR" \
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
  echo "<<BRAT PROCESS ENDED WITH AN ERROR>>\n\n"
  exit 1
}
