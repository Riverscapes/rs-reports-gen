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
Riverscapes Dynamics Rìverscapes Dynamics Rìverscapeś Dynamics Rìverscapeś Dyñamics
Rìverscapeś ðyñamics rìverscapeś ðyñamics rìverŠcapeś ðyñamics rìvÈrŠcapeś ðyñamics
řìvÈrŠcapeś ðyñamics řìvÈŕŠcapeś ðyñamics RìvÈŕŠcapeś ðyñamics RìvÈŕŠcapeś Ðyñamics
RìvÈŕŠcapeś Ðyñamîcs RìvÈŕŠcapÊś Ðyñamîcs RìvÈŕŠcapÊś ÐyñamîcŠ RìvÈŕŚcapÊś ÐyñamîcŠ
RìvÈŕŚcapÊś ÐyñamÌcŠ RìvÈŕŚCapÊś ÐyñamÌcŠ RìvÈŕŚCapÊś ÐÿñamÌcŠ RìvÈŕŚCapÊś ÐYñamÌcŠ
RìvÈŕŚCapÊś ÐYŃamÌcŠ RìvÈŕŚCaPÊś ÐYŃamÌcŠ RìvÈŕŚCaPÊs ÐYŃamÌcŠ RìvÈŕŚCaPÊs ÐYŃamÏcŠ
RìvÈŕšCaPÊs ÐYŃamÏcŠ RìvÈŕšCaPÉs ÐYŃamÏcŠ ŔìvÈŕšCaPÉs ÐYŃamÏcŠ ŔìvÈŕšCaPÉs ÐYŃamÏçŠ
ŔìvÈŕšCaPÉs dYŃamÏçŠ ŔìvÈřšCaPÉs dYŃamÏçŠ ŔìvÈřšCaPÉs dYNamÏçŠ ŔìvÈřšCaPÉs dYNamÏċŠ
ŔìvÈřšCAPÉs dYNamÏċŠ ŔìvÈřŠCAPÉs dYNamÏċŠ ŔîvÈřŠCAPÉs dYNamÏċŠ ŔîvÈřŠCAPÉś dYNamÏċŠ
ŔîvÈřŠCAPÉś dYNamÏčŠ ŔîvÈřŠCAPÉS dYNamÏčŠ ŔîvÈřŠCAPĘS dYNamÏčŠ ŔîvÈřŠCAPĘS dYNamÏčś
ŔîvĘřŠCAPĘS dYNamÏčś ŔîvĘřŠCAPĘS dYNamÏÇś ŔîvĘřŠCAPĘS dýNamÏÇś ŔîvĘřŠCAPĘS DýNamÏÇś
ŔîvĘřŠCAPÉS DýNamÏÇś ŔîvĘřŠCAPÉS DýNaMÏÇś ŔîvĘřŠCAPÉS DýNãMÏÇś ŔîvĘřŠCApÉS DýNãMÏÇś
ŔîvĘřŠcApÉS DýNãMÏÇś ŔîvĘřŠcApÉS DýNãMÏÇŠ ŔîvĘřŠcApÈS DýNãMÏÇŠ ŕîvĘřŠcApÈS DýNãMÏÇŠ
ŕîvĘřŠcApÈS DýNãmÏÇŠ ŕîVĘřŠcApÈS DýNãmÏÇŠ ŕÏVĘřŠcApÈS DýNãmÏÇŠ ŕÏVĘřŠcApÈS DýNàmÏÇŠ
ŕÏVĘřŠcApÈS DýNàmIÇŠ ŕÏVĘřŠcApÈS DŸNàmIÇŠ ŕÏVĘřŠcApÈS ðŸNàmIÇŠ ŕÏvĘřŠcApÈS ðŸNàmIÇŠ                                                                                                                                                                                                                                                                                                                                                                                                                
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
  python -m reports.rpt_riverscapes_dynamics.main \
    "$OUTPUTS_DIR" \
    "$INPUTS_DIR/input.geojson" \
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
