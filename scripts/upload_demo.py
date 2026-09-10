#!/usr/bin/env python3
"""Upload the DEMO Style Guide as a PUBLIC report on the Riverscapes data exchange.

This is the Python-form of the old ``scripts/upload_demo.sh``: it renders the
demo (or reuses an existing output dir), writes the small index.json the API
needs to create a public report, runs the upload, then prints the live URL.

The upload happens under the API's GLOBAL user — that is what makes the report
public. Authentication is browser-based (Auth0) by default, so a browser tab
opens and you must finish the login there while this keeps running. Pass
``--api-key`` (or export ``API_TOKEN``) to use machine auth instead.

Usage::

    uv run python scripts/upload_demo.py                        # build + upload to STAGING
    uv run python scripts/upload_demo.py --stage PRODUCTION     # publish to the live site
    uv run python scripts/upload_demo.py --report-id UUID       # update an existing report
    uv run python scripts/upload_demo.py --dry-run              # build + print plan, no API call

Flags and their environment-var fallbacks (env vars win when the flag is
absent):

    --stage  / STAGE            default STAGING (PRODUCTION for the live site)
    --output-dir / OUTPUT_DIR   default ./demo_output
    --report-id / REPORT_ID     update an existing report instead of creating
    --report-type-id / REPORT_TYPE_ID
                                API report type to create under (must be
                                registered on the target stage). Default:
                                custom-rs-metrics (present on both stages).
    --report-name / REPORT_NAME, --report-description / REPORT_DESCRIPTION
                                title/subtitle of the new public report
    --api-key / API_TOKEN       machine auth token (default: browser auth)
    --skip-build                use the existing output dir as-is
    --html-only                 build only report.html (no static/PDF)
    --dry-run                   do everything except touch the API
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import tempfile
from pathlib import Path

# Make src/ importable when run as `python scripts/upload_demo.py` outside the
# installed package (uv run already does this via the editable install).
REPO_ROOT = Path(__file__).resolve().parent.parent
SRC_DIR = REPO_ROOT / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from api.lib.RSReportsAPI import RSReportsAPI  # noqa: E402
from api.uploadPublic import upload_outputs  # noqa: E402

DEFAULT_NAME = "DEMO Style Guide"
DEFAULT_DESCRIPTION = (
    "Shared component catalog and style guide for Riverscapes reports "
    "(rendered through the exact RSReport pipeline)."
)
DEFAULT_REPORT_TYPE = "custom-rs-metrics"


def _env_or(flag: str, env: str, default: str) -> str:
    """Return the flag value, else the env var, else the default."""
    value = os.environ.get(env)
    return value if value else (flag if flag else default)


def build_demo_output(output_dir: Path, html_only: bool) -> None:
    """Render the demo through the exact RSReport pipeline (in-process)."""
    from util.html.demo.build_demo import build_demo

    print(f"== Building demo ({'HTML only' if html_only else 'interactive + static + PDF'}) ==")
    outputs = build_demo(output_dir, html_only=html_only)
    for path in outputs:
        print(f"  ✔ {path}")


def write_index_json(
    name: str,
    description: str,
    report_type_id: str,
    tmp_dir: Path,
) -> Path:
    """Write the API metadata file (kept outside outputs_dir — it is not an output)."""
    path = tmp_dir / "index.json"
    path.write_text(
        json.dumps(
            {"name": name, "description": description, "reportTypeId": report_type_id},
            indent=2,
        ),
        encoding="utf-8",
    )
    print(f"== index.json for API (not uploaded as an output): {path} ==")
    print(path.read_text(encoding="utf-8"))
    return path


def public_url(stage: str, report_id: str) -> str:
    base = "https://reports.riverscapes.net" if stage.upper() == "PRODUCTION" else "https://staging.reports.riverscapes.net"
    return f"{base}/public/{report_id}"


def main() -> None:
    parser = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument("--stage", help="API stage (default from STAGE env, else STAGING).")
    parser.add_argument("--output-dir", help="Demo output dir (default from OUTPUT_DIR env, else " + str(REPO_ROOT / "demo_output") + ").")
    parser.add_argument("--report-id", help="Upload to an existing report instead of creating one (REPORT_ID env).")
    parser.add_argument("--report-type-id", help=f"API report type to create under (REPORT_TYPE_ID env; default {DEFAULT_REPORT_TYPE}).")
    parser.add_argument("--report-name", help=f"Public report title (REPORT_NAME env; default {DEFAULT_NAME!r}).")
    parser.add_argument("--report-description", help="Public report description (REPORT_DESCRIPTION env).")
    parser.add_argument("--api-key", help="Machine-auth API token (API_TOKEN env). Default: interactive browser auth.")
    parser.add_argument("--skip-build", action="store_true", help="Use the existing output dir as-is.")
    parser.add_argument("--html-only", action="store_true", help="Build only report.html (no static/PDF).")
    parser.add_argument("--dry-run", action="store_true", help="Build + write index.json + print the plan, but do not call the API.")
    args = parser.parse_args()

    stage = _env_or(args.stage, "STAGE", "STAGING").upper()
    output_dir = Path(_env_or(args.output_dir, "OUTPUT_DIR", str(REPO_ROOT / "demo_output")))
    report_id = _env_or(args.report_id, "REPORT_ID", "")
    report_type_id = _env_or(args.report_type_id, "REPORT_TYPE_ID", DEFAULT_REPORT_TYPE)
    report_name = _env_or(args.report_name, "REPORT_NAME", DEFAULT_NAME)
    report_description = _env_or(args.report_description, "REPORT_DESCRIPTION", DEFAULT_DESCRIPTION)
    api_key = _env_or(args.api_key, "API_TOKEN", "")

    print("=" * 60)
    print(" DEMO Style Guide → PUBLIC upload")
    print(f"   Stage:        {stage}")
    print(f"   Output dir:   {output_dir}")
    if report_id:
        print(f"   Report ID:    {report_id} (updating existing)")
    else:
        print("   Report ID:    (will create a new public report)")
        print(f"   Report type:  {report_type_id}")
    print("=" * 60)

    # 1. Build (or reuse) the demo.
    if args.skip_build:
        if not (output_dir / "report.html").is_file():
            parser.error(f"--skip-build but {output_dir}/report.html does not exist.")
        print("== Reusing existing output (--skip-build) ==")
    else:
        output_dir.mkdir(parents=True, exist_ok=True)
        build_demo_output(output_dir, args.html_only)

    output_files = [p for p in output_dir.rglob("*") if p.is_file()]
    if not output_files:
        parser.error(f"No output files found under {output_dir}.")
    print(f"== Upload bundle: {len(output_files)} file(s) under {output_dir} ==")

    # 2. index.json — only needed when creating a NEW report.
    tmp_dir = Path(tempfile.mkdtemp(prefix="demo_upload_", dir="/tmp"))
    index_json = write_index_json(report_name, report_description, report_type_id, tmp_dir) if not report_id else None

    # 3. Plan / execute.
    if args.dry_run:
        print("== DRY RUN — would execute (in-process, no subprocess) ==")
        action = f"create report '{report_name}' (type {report_type_id}) and upload" if index_json else f"upload to existing report {report_id}"
        print(f"   {action} {len(output_files)} file(s) from {output_dir} to stage {stage}")
        print("   auth: " + ("machine token (--api-key)" if api_key else "browser tab (Auth0). It would open a browser for login."))
        print("== DRY RUN — no upload performed. ==")
        return

    with RSReportsAPI(stage=stage, api_token=api_key or None) as api_client:
        # upload_outputs creates the report when report_id is empty and
        # index_json is given (no interactive prompts), returns the report ID.
        report_id = upload_outputs(
            api_client=api_client,
            outputs_dir=output_dir,
            index_json=index_json,
            report_id=report_id or None,
        )

    print("")
    print("✔ Upload complete. Public report URL:")
    print(f"    {public_url(stage, report_id)}")


if __name__ == "__main__":
    main()