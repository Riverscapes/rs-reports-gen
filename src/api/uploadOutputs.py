"""Upload report outputs to the Riverscapes API."""

from __future__ import annotations

import argparse
import logging
import os
import sys
import traceback
from typing import Dict, List, Tuple

import requests
from rsxml import Logger, dotenv

from .RSReportsAPI import RSReportsAPI


def _collect_output_files(outputs_dir: str, log_only: bool = False) -> List[Tuple[str, str]]:
    """Return a list of ``(local_path, s3_path)`` tuples for files in ``outputs_dir``."""

    collected: List[Tuple[str, str]] = []
    for root, _dirs, files in os.walk(outputs_dir):
        for filename in files:
            local_path = os.path.join(root, filename)
            relative_path = os.path.relpath(local_path, outputs_dir)
            s3_path = relative_path.replace(os.sep, "/")
            if log_only and not filename.lower().endswith((".log")):
                continue
            collected.append((local_path, s3_path))
    return collected


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

    files_to_upload = _collect_output_files(outputs_dir, log_only=log_only)

    if not files_to_upload:
        log.warning("No output files found to upload.")
        return []

    with RSReportsAPI(api_token=api_key, stage=stage) as api_client:
        query = api_client.load_query("GetUploadUrls")
        variables = {
            "userId": user_id,
            "reportId": report_id,
            "filePaths": [remote for _local, remote in files_to_upload],
            "fileType": "OUTPUTS",
        }
        results = api_client.run_query(query, variables)

    upload_entries = results.get("data", {}).get("uploadUrls", []) if results else []
    if not upload_entries:
        raise RuntimeError("API did not return any upload URLs.")

    # Map entries by the object key when supplied in the fields payload.
    entries_by_key: Dict[str, Dict] = {}
    sequential_entries: List[Dict] = []
    for entry in upload_entries:
        fields = entry.get("fields") if isinstance(entry, dict) else None
        if isinstance(fields, dict):
            key = fields.get("key") or fields.get("Key")
            if key:
                entries_by_key[key] = entry
        sequential_entries.append(entry)

    uploaded: List[str] = []
    for index, (local_path, remote_path) in enumerate(files_to_upload):
        entry = entries_by_key.get(remote_path)
        if entry is None and index < len(sequential_entries):
            entry = sequential_entries[index]
        if entry is None:
            log.warning("No upload URL found for %s", remote_path)
            continue

        url = entry.get("url") if isinstance(entry, dict) else None
        fields = entry.get("fields") if isinstance(entry, dict) else None
        if not url:
            log.warning("Missing upload URL in API response for %s", remote_path)
            continue

        log.info(f"Uploading {local_path} -> {url.split('?')[0]}")
        with open(local_path, "rb") as data_stream:
            if isinstance(fields, dict) and fields:
                # S3 pre-signed POST upload.
                files = {"file": (os.path.basename(local_path), data_stream)}
                try:
                    response = requests.post(url, data=fields, files=files, timeout=900)
                except requests.Timeout:
                    log.error(f"Request timed out after 900 seconds: {url}")
                    raise RuntimeError(f"Failed to upload {local_path} due to timeout") from None
                except requests.RequestException as e:
                    log.error(f"Error occurred while uploading {local_path}: {e}")
                    raise RuntimeError(f"Failed to upload {local_path}") from e
            else:
                # Fallback for pre-signed PUT uploads.
                try:
                    response = requests.put(url, data=data_stream, timeout=900)
                except requests.Timeout:
                    log.error(f"Request timed out after 900 seconds: {url}")
                    raise RuntimeError(f"Failed to upload {local_path} due to timeout") from None
                except requests.RequestException as e:
                    log.error(f"Error occurred while uploading {local_path}: {e}")
                    raise RuntimeError(f"Failed to upload {local_path}") from e
        response.raise_for_status()
        uploaded.append(remote_path)


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
