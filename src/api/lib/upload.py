"""Upload report outputs to the Riverscapes API."""

from __future__ import annotations

from typing import Dict, List, Tuple

import requests
from rsxml import Logger

from api.lib.RSReportsAPI import RSReportsAPI
from api.lib.multipart_upload import stream_post_file


def upload_files(api_client: RSReportsAPI, user_id: str, report_id: str, files_to_upload: List[Tuple[str, str]], timeout: int = 900):
    """Upload files to the Riverscapes API.

    Args:
        api_client (RSReportsAPI): The API client to use for uploading files.
        user_id (str): The ID of the user uploading the files.
        report_id (str): The ID of the report the files are associated with.
        files_to_upload (List[Tuple[str, str]]): A list of tuples containing local and remote file paths.
        timeout (int, optional): The timeout for the upload request in seconds. Defaults to 900.

    Raises:
        RuntimeError: If the API does not return any upload URLs.
        RuntimeError: If the upload request fails.
        RuntimeError: If the uploaded file is not found.

    """
    log = Logger("Upload Files")
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
        try:
            if isinstance(fields, dict) and fields:
                response = stream_post_file(
                    url,
                    fields=fields,
                    file_path=local_path,
                    timeout=timeout,
                )
            else:
                with open(local_path, "rb") as data_stream:
                    response = requests.put(url, data=data_stream, timeout=timeout)
            response.raise_for_status()
        except requests.Timeout:
            log.error(f"Request timed out after {timeout} seconds: {url}")
            raise RuntimeError(f"Failed to upload {local_path} due to timeout") from None
        except requests.RequestException as exc:
            log.error(f"Error occurred while uploading {local_path}: {exc}")
            raise RuntimeError(f"Failed to upload {local_path}") from exc
        uploaded.append(remote_path)
