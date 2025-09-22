"""Upload report outputs to the Riverscapes API."""

from __future__ import annotations

import argparse
import logging
import os
import sys
import json
import traceback
from typing import Dict, List, Tuple
from termcolor import colored
import inquirer

import requests
from rsxml import Logger, dotenv

from .RSReportsAPI import RSReportsAPI

GLOBAL_USER_ID = 'GLOBAL'


def _collect_output_files(outputs_dir: str) -> List[Tuple[str, str]]:
    """Return a list of ``(local_path, s3_path)`` tuples for files in ``outputs_dir``."""

    collected: List[Tuple[str, str]] = []
    for root, _dirs, files in os.walk(outputs_dir):
        for filename in files:
            local_path = os.path.join(root, filename)
            relative_path = os.path.relpath(local_path, outputs_dir)
            s3_path = relative_path.replace(os.sep, "/")
            collected.append((local_path, s3_path))
    return collected


def upload_outputs(
    api_client: RSReportsAPI,
    outputs_dir: str = None,
    index_json: str = None,
    report_id: str = None,
) -> List[str]:
    """ Upload a public project report to the API.

    Args:
        api_client (RSReportsAPI): Instance of RSReportsAPI.
        outputs_dir (str, optional): Path to the directory containing output files. Defaults to None.
        index_json (str, optional): Path to the index JSON file. Defaults to None.
        report_id (str, optional): ID of the report to upload files to. Defaults to None.
    """

    log = Logger("Upload Outputs")
    log.title("API Upload Outputs")

    if not report_id:
        log.info("No report ID supplied")
        questions = [
            inquirer.Confirm('create_new', message="Create a new report?", default=True),
        ]
        answers = inquirer.prompt(questions)
        if not answers.get('create_new', True):
            questions = [
                inquirer.Text('report_id', message="Enter the report ID"),
            ]
            answers = inquirer.prompt(questions)
            report_id = answers.get('report_id')
            if not report_id:
                log.error('You need to supply a report ID or create a new report')
                raise RuntimeError("No report ID supplied")

    # If the user didn't provide these via args or env, prompt for them
    if not outputs_dir:
        questions = [
            inquirer.Text('outputs_dir', message="Path to the directory containing output files"),
        ]
        answers = inquirer.prompt(questions)
        outputs_dir = answers['outputs_dir']
        if not os.path.isdir(outputs_dir):
            log.error('You need to supply a valid path to the directory containing output files')
            raise RuntimeError("No valid outputs directory supplied")

    # If there's no report ID then we are replacing a report, not creating a new one
    if not report_id:

        # If there's no index JSON then we need to prompt for it
        if not index_json:
            log.info('You need to supply a path to a JSON file with the report parameters')
            log.info('The format of this json file should be: ')
            log.info(colored(json.dumps({
                "name": "Report name",
                "description": "Report description",
                "reportTypeId": "Report type ID (e.g., rs-hydrofabric)"
            }, ), 'cyan'))

            questions = [
                inquirer.Text('index_json', message="Path to index JSON file"),
            ]
            answers = inquirer.prompt(questions)
            index_json = answers['index_json']

        if not os.path.isfile(index_json) or not index_json.endswith('.json'):
            log.info('You need to supply a valid path to a JSON file with the index parameters')
            raise RuntimeError("No valid index JSON file supplied")

        with open(index_json, 'r', encoding='utf-8') as f:
            index_data = json.load(f)
        if not isinstance(index_data, dict):
            raise RuntimeError("Index JSON file does not contain a valid JSON object")
        name = index_data.get("name")
        description = index_data.get("description")
        report_type_id = index_data.get("reportTypeId")
        if not name or not description or not report_type_id:
            raise RuntimeError("Index JSON file is missing required fields: name, description, reportTypeId")

        create_mutation = api_client.load_mutation("CreateReport")
        create_variables = {
            "userId": GLOBAL_USER_ID,
            "project": {
                "name": name,
                "description": description,
                "reportTypeId": report_type_id
            }
        }
        create_res = api_client.run_query(create_mutation, create_variables)
        if not create_res or "errors" in create_res:
            raise RuntimeError(f"API CreateReport mutation failed: {create_res}")
        report_id = create_res.get("data", {}).get("createReport", {}).get("report", {}).get("id")
        if not report_id:
            raise RuntimeError(f"API CreateReport mutation did not return a report ID: {create_res}")
        log.info(f"Created new report with ID: {report_id}")

    files_to_upload = _collect_output_files(outputs_dir)
    if not files_to_upload:
        log.warning("No output files found to upload.")
        return []

    query = api_client.load_query("GetUploadUrls")
    variables = {
        "userId": GLOBAL_USER_ID,
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
                response = requests.post(url, data=fields, files=files, timeout=120)
            else:
                # Fallback for pre-signed PUT uploads.
                response = requests.put(url, data=data_stream, timeout=120)
        response.raise_for_status()
        uploaded.append(remote_path)

    # Now call StartUpload mutation
    # NOTE: We can get fancier with messaging but for now the Cybercastor task completion should
    # signal the completion of the report
    # log.info("Notifying API of completed uploads")
    update_mutation = api_client.load_mutation("UpdateReport")
    update_variables = {
        "userId": GLOBAL_USER_ID,
        "reportId": report_id,
        "report": {
            "progress": 100,
            "statusMessage": "Upload complete",
            "status": "COMPLETE",
        },
    }
    start_res = api_client.run_query(update_mutation, update_variables)
    if not start_res or "errors" in start_res:
        raise RuntimeError(f"API StartUpload mutation failed: {start_res}")
    log.info("API StartUpload mutation successful")


def main() -> None:
    """CLI entry point for uploading report outputs."""

    parser = argparse.ArgumentParser()
    parser.add_argument("--outputs_dir", help="Directory containing output files to upload.", type=str, default=None)
    parser.add_argument("--index_json", help="Path to the index JSON file.", type=str, default=None)
    parser.add_argument("--stage", help="API stage (falls back to STAGE in environment).", type=str)
    parser.add_argument("--report-id", help="Report ID (If none specified then it will walk you through creating a new report).", type=str, default=None)

    args = dotenv.parse_args_env(parser)

    stage = args.stage if args.stage else os.getenv("STAGE")

    if not stage:
        print("No stage supplied or found in environment as STAGE")
        sys.exit(1)

    log = Logger("Upload Public Setup")
    log.setup(log_level=logging.DEBUG)

    try:
        with RSReportsAPI(stage=stage) as api_client:
            upload_outputs(
                api_client=api_client,
                outputs_dir=args.outputs_dir,
                index_json=args.index_json,
                report_id=args.report_id,
            )
        sys.exit(0)
    except Exception as exc:  # pragma: no cover - CLI safety net
        log.error(exc)
        traceback.print_exc(file=sys.stdout)
        sys.exit(1)


if __name__ == "__main__":
    main()
