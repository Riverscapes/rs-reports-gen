"""Main entry point"""

# Standard library imports
import os
import argparse
import logging
import sys
import traceback
from urllib.parse import urlparse
# Third party imports
import requests
from rsxml import Logger, dotenv
from rsxml.util import safe_makedirs

from api.lib.RSReportsAPI import RSReportsAPI


def download_inputs(inputs_dir: str, api_key: str, user_id: str, report_id: str, stage: str):
    """
    Use the API to download inputs for a report

    """
    log = Logger('Download Inputs')
    log.title('API Download Inputs')

    with RSReportsAPI(api_token=api_key, stage=stage) as api_client:
        qry = api_client.load_query('GetDownloadUrls')
        results = api_client.run_query(
            qry,
            variables={'userId': user_id, 'reportId': report_id, 'fileTypes': ['INPUTS', 'INDEX']}
        )

    download_urls = results.get('data', {}).get('downloadUrls', []) if results else []

    if not download_urls:
        log.warning('No input files available for download.')
        return

    for file_meta in download_urls:
        url = file_meta.get('url')
        if not url:
            log.warning('Download URL missing in API response: %s', file_meta)
            continue

        parsed_url = urlparse(url)
        path_parts = [part for part in parsed_url.path.split('/') if part]

        relative_parts = []
        try:
            reports_index = path_parts.index('REPORTS')
            relative_parts = path_parts[reports_index + 2:]
        except ValueError:
            pass

        if not relative_parts:
            basename = os.path.basename(parsed_url.path)
            relative_parts = [basename] if basename else []

        if not relative_parts:
            log.warning('Unable to determine filename from URL: %s', url)
            continue

        local_path = os.path.join(inputs_dir, *relative_parts)
        parent_dir = os.path.dirname(local_path)
        if parent_dir:
            safe_makedirs(parent_dir)

        log.info(f'Downloading {url.split("?")[0]} -> {local_path}')
        response = requests.get(url, stream=True, timeout=120)
        response.raise_for_status()
        with open(local_path, 'wb') as output_file:
            for chunk in response.iter_content(chunk_size=8192):
                if chunk:
                    output_file.write(chunk)
        log.debug(f'Downloaded {local_path} ({os.path.getsize(local_path)} bytes)')

    return


def main():
    """
    API Operation: Download Inputs
    """
    parser = argparse.ArgumentParser()
    parser.add_argument('inputs_dir', help='Directory containing input files.', type=str)
    parser.add_argument('--api-key', help='(optional) API Token. If not supplied, will be read from .env as API_TOKEN', type=str)
    parser.add_argument('--user-id', help='(optional) User ID for the report. If not supplied, will be read from .env as USER_ID', type=str)
    parser.add_argument('--report-id', help='(optional) Report ID for the report. If not supplied, will be read from .env as REPORT_ID', type=str)
    parser.add_argument('--stage', help='(optional) Stage for the report. If not supplied, will be read from .env as STAGE', type=str)

    args = dotenv.parse_args_env(parser)

    api_key = args.api_key if args.api_key else os.getenv('API_TOKEN', None)
    user_id = args.user_id if args.user_id else os.getenv('USER_ID', None)
    report_id = args.report_id if args.report_id else os.getenv('REPORT_ID', None)
    if not api_key:
        print("No API Token supplied or found in environment as API_TOKEN")
        sys.exit(1)
    if not user_id:
        print("No User ID supplied or found in environment as USER_ID")
        sys.exit(1)
    if not report_id:
        print("No Report ID supplied or found in environment as REPORT_ID")
        sys.exit(1)

    safe_makedirs(args.inputs_dir)

    log = Logger('Setup')
    log_path = os.path.join(args.inputs_dir, 'athena-rme-scrape.log')
    log.setup(log_path=log_path, log_level=logging.DEBUG)

    try:
        download_inputs(args.inputs_dir, api_key=api_key, user_id=user_id, report_id=report_id, stage=args.stage)
        print("done")
        sys.exit(0)

    except Exception as e:
        log.error(e)
        traceback.print_exc(file=sys.stdout)
        sys.exit(1)


if __name__ == '__main__':
    main()
