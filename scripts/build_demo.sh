#!/usr/bin/env bash
# Render the DEMO style-guide report (see src/util/html/demo/README.md).
# All arguments are forwarded to rs-report-demo.
set -euo pipefail
SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
cd "$SCRIPT_DIR/.."
uv run rs-report-demo "$@"
