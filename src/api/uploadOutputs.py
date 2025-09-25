"""Upload report outputs to the Riverscapes API."""

from __future__ import annotations

import argparse
import logging
import os
import sys
import traceback
from typing import List

from rsxml import Logger, dotenv

from api.lib.RSReportsAPI import RSReportsAPI
from api.lib.file_utils import collect_output_files
from api.lib.upload import upload_files


DEFAULT_UPLOAD_TIMEOUT = 900


def upload_outputs(
    outputs_dir: str,
    api_key: str,
    user_id: str,
    report_id: str,
    stage: str,
    log_only: bool = False,
) -> List[str]:
    """Upload all files in ``outputs_dir`` as ``file_type`` for ``report_id``."""

    log = Logger("Upload Outputs")
    log.title("API Upload Outputs")

    files_to_upload = collect_output_files(outputs_dir, log_only=log_only)
    if not files_to_upload:
        log.warning("No output files found to upload.")
        return []

    timeout_value = DEFAULT_UPLOAD_TIMEOUT

    with RSReportsAPI(api_token=api_key, stage=stage) as api_client:
        upload_files(api_client, user_id, report_id, files_to_upload, timeout=timeout_value)


def main() -> None:
    """CLI entry point for uploading report outputs."""

    parser = argparse.ArgumentParser()
    parser.add_argument("outputs_dir", help="Directory containing output files to upload.", type=str)
    parser.add_argument("--api-key", help="API token (falls back to API_TOKEN in environment).", type=str)
    parser.add_argument("--user-id", help="User ID (falls back to USER_ID in environment).", type=str)
    parser.add_argument("--report-id", help="Report ID (falls back to REPORT_ID in environment).", type=str)
    parser.add_argument("--log-only", help="Just upload logs.", action="store_true", default=False)
    parser.add_argument("--stage", help="API stage (falls back to STAGE in environment).", type=str)

    args = dotenv.parse_args_env(parser)

    api_key = args.api_key if args.api_key else os.getenv("API_TOKEN")
    user_id = args.user_id if args.user_id else os.getenv("USER_ID")
    report_id = args.report_id if args.report_id else os.getenv("REPORT_ID")
    stage = args.stage if args.stage else os.getenv("STAGE")

    if not api_key:
        print("No API token supplied or found in environment as API_TOKEN")
        sys.exit(1)
    if not user_id:
        print("No User ID supplied or found in environment as USER_ID")
        sys.exit(1)
    if not report_id:
        print("No Report ID supplied or found in environment as REPORT_ID")
        sys.exit(1)
    if not stage:
        print("No stage supplied or found in environment as STAGE")
        sys.exit(1)

    if not os.path.isdir(args.outputs_dir):
        print(f"Outputs directory does not exist: {args.outputs_dir}")
        sys.exit(1)

    log = Logger("Upload Setup")
    log_path = os.path.join(args.outputs_dir, "upload-outputs.log")
    log.setup(log_path=log_path, log_level=logging.DEBUG)

    try:
        upload_outputs(
            args.outputs_dir,
            api_key=api_key,
            user_id=user_id,
            report_id=report_id,
            stage=stage,
            log_only=args.log_only
        )
        sys.exit(0)
    except Exception as exc:  # pragma: no cover - CLI safety net
        log.error(exc)
        traceback.print_exc(file=sys.stdout)
        sys.exit(1)


if __name__ == "__main__":
    main()
