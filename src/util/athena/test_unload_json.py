"""Temporary helpers for poking at Athena."""

import gzip
import io
import json
import time
from urllib.parse import urlparse

import boto3
from rsxml import Logger

from util.athena.athena import ATHENA_WORKGROUP, S3_ATHENA_BUCKET_OUTPUT


def unload_query_to_json(select_query: str, output_prefix: str) -> list[dict]:
    """_summary_

    Args:
        select_query (str): select query to run
        output_prefix (str): s3 prefix to write to (must be s3://bucket/prefix/)
        fetch_data (bool, optional): whether to fetch the data from S3 after unloading. Defaults to False.

    Raises:
        ValueError: _description_
        TimeoutError: _description_
        RuntimeError: _description_
        ValueError: _description_

    Returns:
        list[dict]: _description_
    """

    log = Logger("Athena UNLOAD")
    compression = "GZIP"
    region = "us-west-2"
    athena = boto3.client("athena", region_name=region)

    cleaned_query = select_query.strip().rstrip(";")
    output_s3_path = f's3://{S3_ATHENA_BUCKET_OUTPUT}/{output_prefix}'
    output_s3_path = output_s3_path if output_s3_path.endswith("/") else output_s3_path + "/"

    # Start the UNLOAD query
    log.info(f"Running UNLOAD to {output_s3_path}")
    start_response = athena.start_query_execution(
        QueryString=(
            f"UNLOAD ({cleaned_query}) TO '{output_s3_path}' "
            f"WITH (format='JSON', compression='GZIP')"
        ),
        QueryExecutionContext={
            'Database': 'default',
            'Catalog': 'AwsDataCatalog'
        },
        ResultConfiguration={"OutputLocation": output_s3_path},
        WorkGroup=ATHENA_WORKGROUP,
    )
    query_execution_id = start_response["QueryExecutionId"]

    start_time = time.time()
    state_change_reason = None

    # Poll the query status until it completes or fails
    while True:
        status_response = athena.get_query_execution(QueryExecutionId=query_execution_id)
        state = status_response["QueryExecution"]["Status"]["State"]
        state_change_reason = status_response["QueryExecution"]["Status"].get("StateChangeReason")
        if state in {"SUCCEEDED", "FAILED", "CANCELLED"}:
            break
        if time.time() - start_time > 600:
            raise TimeoutError(f"Athena UNLOAD {query_execution_id} did not finish within 600 seconds")
        time.sleep(3)

    if state != "SUCCEEDED":
        message = state_change_reason or "No reason provided"
        raise RuntimeError(f"Athena UNLOAD {query_execution_id} ended with state '{state}': {message}")

    parsed = urlparse(output_s3_path)
    if parsed.scheme != "s3" or not parsed.netloc:
        raise ValueError("output_s3_path must be an S3 URI, e.g. s3://bucket/prefix/")

    prefix_key = parsed.path.lstrip("/")

    # Now we need to list and fetch the results. We CAN use the manifest file but it's probably easier just
    # to list all the .gz files and read them in order we find them.

    s3 = boto3.client("s3", region_name=region)
    paginator = s3.get_paginator("list_objects_v2")
    file_keys: list[str] = []

    # Limit to just the .gz files
    for page in paginator.paginate(Bucket=S3_ATHENA_BUCKET_OUTPUT, Prefix=prefix_key):
        for obj in page.get("Contents", []):
            key = obj["Key"]
            if key.endswith(".json") or key.endswith(".gz"):
                file_keys.append(key)

    rows = []
    # Now fetch, decompress and parse each file
    for key in file_keys:
        response = s3.get_object(Bucket=S3_ATHENA_BUCKET_OUTPUT, Key=key)
        body = response["Body"].read()
        if compression.upper() == "GZIP":
            try:
                with gzip.GzipFile(fileobj=io.BytesIO(body)) as gz:
                    for raw_line in gz:
                        line = raw_line.decode("utf-8").strip()
                        if line:
                            rows.append(json.loads(line))
            except gzip.BadGzipFile:
                log.error(f"File {key} is not a valid gzip file")

    return rows


if __name__ == "__main__":

    result = unload_query_to_json(
        """
        SELECT * FROM rs_context_huc10 LIMIT 300
        """,
        output_prefix=f"matt/{int(time.time())}/",
    )
    print(json.dumps(result, indent=2))
