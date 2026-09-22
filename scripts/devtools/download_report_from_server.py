"""Download report from riverscapes.reports.net or staging version
Also gets the index.json and inputs.json

"""

import argparse
import json
import os
import uuid
from datetime import UTC, datetime
from pathlib import Path
from zoneinfo import ZoneInfo

import boto3
import questionary

AWS_REGION = "us-west-2"
VANCOUVER_TZ = ZoneInfo('America/Vancouver')


def _validate_report_id(report_id: str) -> str:
    """Validate and normalize report_id as a GUID string."""
    try:
        return str(uuid.UUID(report_id))
    except ValueError as exc:
        raise ValueError(f'Invalid report_id GUID: {report_id}') from exc


def parse_args() -> dict:
    """Parse CLI args or collect them interactively with questionary."""
    parser = argparse.ArgumentParser(description='Download report artifacts from S3.')
    parser.add_argument('-i', '--interactive', action='store_true', help='Prompt for arguments using questionary.')
    parser.add_argument('--report-env', choices=['staging', 'production'], help='Report environment.')
    parser.add_argument('--report-id', help='Report GUID to download.')
    parser.add_argument('--user-id', help='User ID for S3 report paths. Falls back to DEX_USER_ID when omitted.')
    parsed = parser.parse_args()

    report_env = parsed.report_env
    report_id = parsed.report_id
    user_id = parsed.user_id or os.getenv('DEX_USER_ID')

    if not user_id:
        user_id = questionary.text(
            'Enter user ID (DEX user id):',
            validate=lambda val: True if val and val.strip() else 'User ID is required.',
        ).ask()

    if parsed.interactive:
        report_env = questionary.select(
            'Select report environment:',
            choices=['staging', 'production'],
            default=report_env or 'staging',
        ).ask()

        bucket = _bucket_for_report_env(report_env)
        if not bucket:
            parser.error(f'No bucket configured for report environment: {report_env}')

        available_report_ids = list_available_report_ids(bucket=bucket, user_id=user_id)

        if available_report_ids:
            default_report_id = report_id if report_id in available_report_ids else available_report_ids[0]
            report_id = questionary.select(
                'Select report ID from S3 index:',
                choices=available_report_ids,
                default=default_report_id,
            ).ask()
        else:
            report_id = questionary.text(
                'Enter report ID (GUID):',
                default=report_id or '',
                validate=lambda val: True if _is_valid_guid(val) else 'Enter a valid GUID.',
            ).ask()

    if not report_env:
        parser.error('--report-env is required unless --interactive is used.')
    if not report_id:
        parser.error('--report-id is required unless --interactive is used.')
    if not user_id:
        parser.error('User ID is required. Provide --user-id or set DEX_USER_ID.')

    return {
        'report_env': report_env,
        'report_id': _validate_report_id(report_id),
        'user_id': user_id,
        'local_base_path': Path(os.getenv('DATA_ROOT', '')),
    }


def _is_valid_guid(value: str) -> bool:
    """Return True when value parses as a GUID, else False."""
    try:
        uuid.UUID(value)
        return True
    except ValueError:
        return False


def _bucket_for_report_env(env: str) -> str:
    """Return the bucket name for the selected report environment."""
    if env == 'staging':
        return os.getenv('STAGING_BUCKET', "")
    return os.getenv('PRODUCTION_BUCKET', "")


def list_available_report_ids(bucket: str, user_id: str) -> list[str]:
    """List report IDs discovered under S3 index prefixes for the given user."""
    s3_client = boto3.client('s3', region_name=AWS_REGION)
    prefixes = [
        f'USERS/INDEX/{user_id}/REPORTS/',
        f'users/index/{user_id}/REPORTS/',
    ]
    report_ids: set[str] = set()

    for prefix in prefixes:
        paginator = s3_client.get_paginator('list_objects_v2')
        for page in paginator.paginate(Bucket=bucket, Prefix=prefix, Delimiter='/'):
            for child in page.get('CommonPrefixes', []):
                child_prefix = child.get('Prefix', '')
                if not child_prefix.startswith(prefix):
                    continue
                report_id = child_prefix.removeprefix(prefix).strip('/')
                if _is_valid_guid(report_id):
                    report_ids.add(report_id)

    return sorted(report_ids)


def read_json_document(bucket: str, key: str) -> dict:
    """Read a document-style JSON object from S3 and return it as a dict."""
    s3_client = boto3.client('s3', region_name=AWS_REGION)
    response = s3_client.get_object(Bucket=bucket, Key=key)
    body_text = response['Body'].read().decode('utf-8')
    return json.loads(body_text)


def ts_to_label(timestamp: int) -> str:
    """convert timestamp such as 1790029763920 to formatted YYYYMMDD-HHMM for file path in Vancouver Time"""
    # Accept either epoch milliseconds or epoch seconds.
    epoch_seconds = timestamp / 1000 if timestamp > 10**11 else timestamp
    vancouver_dt = datetime.fromtimestamp(epoch_seconds, tz=UTC).astimezone(VANCOUVER_TZ)
    return vancouver_dt.strftime('%Y%m%d-%H%M')


def download_s3_prefix_children(bucket: str, prefix: str, local_dir: Path) -> int:
    """Download all object children under an S3 prefix into a local directory tree."""
    s3_client = boto3.client('s3', region_name=AWS_REGION)
    paginator = s3_client.get_paginator('list_objects_v2')
    local_dir.mkdir(parents=True, exist_ok=True)

    download_count = 0
    normalized_prefix = f'{prefix.rstrip("/")}/'

    for page in paginator.paginate(Bucket=bucket, Prefix=normalized_prefix):
        for obj in page.get('Contents', []):
            key = obj.get('Key', '')
            if not key or key.endswith('/'):
                continue

            rel_path = Path(key.removeprefix(normalized_prefix))
            destination = local_dir / rel_path
            destination.parent.mkdir(parents=True, exist_ok=True)

            s3_client.download_file(bucket, key, str(destination))
            download_count += 1

    return download_count


def download_report(report_env: str, report_id: str, user_id: str, local_base_path: Path):
    """Download report metadata and decide local storage path from index JSON."""
    inputs_s3key = f"USERS/INPUTS/{user_id}/REPORTS/{report_id}"
    index_s3key = f"USERS/INDEX/{user_id}/REPORTS/{report_id}/index.json"
    outputs_s3key = f"USERS/OUTPUTS/{user_id}/REPORTS/{report_id}"

    bucket = _bucket_for_report_env(report_env)
    # Read as document JSON, not a tabular DataFrame.
    index_json = read_json_document(bucket=bucket, key=index_s3key)

    report_type = index_json.get('reportTypeId') or 'unknown_report'
    report_date = int(index_json.get('createdAtTS', 0))
    report_date_label = ts_to_label(report_date)

    save_dir = local_base_path / 'reports-downloads' / report_type / f"{report_date_label}-{report_env}"
    save_dir.mkdir(parents=True, exist_ok=True)
    (save_dir / 'index.json').write_text(json.dumps(index_json, indent=2), encoding='utf-8')

    outputs_local_dir = save_dir / 'outputs'
    outputs_download_count = download_s3_prefix_children(bucket=bucket, prefix=outputs_s3key, local_dir=outputs_local_dir)
    inputs_local_dir = save_dir / 'inputs'
    inputs_download_count = download_s3_prefix_children(bucket=bucket, prefix=inputs_s3key, local_dir=inputs_local_dir)

    print(f"Saved index JSON to {save_dir / 'index.json'}")
    print(f"Downloaded {inputs_download_count} input files to {inputs_local_dir}")
    print(f"Downloaded {outputs_download_count} output files to {outputs_local_dir}")


def main():
    """Get arguments and execute download"""
    args = parse_args()
    download_report(report_id=args['report_id'], user_id=args['user_id'], local_base_path=args['local_base_path'], report_env=args['report_env'])


if __name__ == "__main__":
    main()
