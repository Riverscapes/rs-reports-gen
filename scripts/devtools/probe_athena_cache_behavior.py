"""Probe awswrangler Athena cache behavior with repeated query execution.

Date created: 2026-11-05
Created by copilot.

Usage examples:
    uv run python ./scripts/devtools/probe_athena_cache_behavior.py --query "SELECT 1"
    uv run python ./scripts/devtools/probe_athena_cache_behavior.py --query-file ./tmp/query.sql --runs 5 --pause-seconds 1

Notes:
    - Uses the same region/session settings as util.athena.athena.
    - Runs two modes by default: no_cache and cache.
    - Prints per-run wall time and Athena query metadata so behavior is visible without full report runs.
"""

import argparse
import time
import uuid
from pathlib import Path
from typing import Any

import awswrangler as wr

from util.athena import athena as athena_util


def _parse_args() -> argparse.Namespace:
    """Parse CLI args for cache probe runs."""
    parser = argparse.ArgumentParser(description="Run repeated Athena queries with/without awswrangler cache settings.")
    parser.add_argument("--query", help="SQL query string to execute.")
    parser.add_argument("--query-file", help="Path to a .sql file containing the query.")
    parser.add_argument("--database", default="default", help="Athena database for query context. Default: default")
    parser.add_argument("--runs", type=int, default=3, help="Number of runs per mode. Default: 3")
    parser.add_argument("--pause-seconds", type=float, default=0.0, help="Optional pause between runs in each mode.")
    parser.add_argument(
        "--mode",
        choices=["both", "cache", "no_cache"],
        default="both",
        help="Which mode(s) to execute. Default: both",
    )
    parser.add_argument(
        "--use-unload",
        action="store_true",
        default=True,
        help="Use unload_approach=True, ctas_approach=False (matches report path).",
    )
    parser.add_argument(
        "--no-use-unload",
        dest="use_unload",
        action="store_false",
        help="Use regular CSV path (ctas_approach=False, unload_approach=False).",
    )
    return parser.parse_args()


def _load_query(args: argparse.Namespace) -> str:
    """Load query text from --query or --query-file and validate input."""
    if bool(args.query) == bool(args.query_file):
        raise ValueError("Provide exactly one of --query or --query-file.")

    if args.query:
        query = args.query.strip()
    else:
        query = Path(args.query_file).read_text(encoding="utf-8").strip()

    if not query:
        raise ValueError("Query text is empty.")
    return query.rstrip(";")


def _extract_metadata(df: Any) -> dict[str, Any]:
    """Extract key Athena metadata fields from a DataFrame query result."""
    query_metadata = getattr(df, "query_metadata", {}) or {}
    stats = query_metadata.get("Statistics", {}) if isinstance(query_metadata, dict) else {}
    status = query_metadata.get("Status", {}) if isinstance(query_metadata, dict) else {}
    reuse = stats.get("ResultReuseInformation", {}) if isinstance(stats, dict) else {}

    return {
        "reused_previous_result": reuse.get("ReusedPreviousResult"),
        "data_scanned_bytes": stats.get("DataScannedInBytes"),
        "execution_time_ms": stats.get("TotalExecutionTimeInMillis"),
        "query_execution_id": query_metadata.get("QueryExecutionId"),
        "athena_state": status.get("State"),
    }


def _run_single(
    *,
    query: str,
    database: str,
    use_cache: bool,
    use_unload: bool,
) -> dict[str, Any]:
    """Run one query execution and return timing plus metadata."""
    s3_output = f"s3://{athena_util.S3_ATHENA_BUCKET}/athena_unload/{uuid.uuid4()}/"
    cache_settings = athena_util.ATHENA_CACHE_SETTINGS if use_cache else None

    start = time.perf_counter()
    df = wr.athena.read_sql_query(
        query,
        database=database,
        ctas_approach=False,
        unload_approach=use_unload,
        s3_output=s3_output if use_unload else None,
        athena_cache_settings=cache_settings,
        boto3_session=athena_util.get_boto3_session(),
    )
    elapsed_ms = (time.perf_counter() - start) * 1000

    metadata = _extract_metadata(df)
    metadata["row_count"] = len(df.index)
    metadata["elapsed_ms"] = round(elapsed_ms, 1)
    return metadata


def _print_header() -> None:
    """Print tabular output header."""
    print("mode\trun\telapsed_ms\texecution_time_ms\tdata_scanned_bytes\treused_previous_result\trow_count\tathena_state\tquery_execution_id")


def _run_mode(
    *,
    mode_name: str,
    query: str,
    database: str,
    runs: int,
    pause_seconds: float,
    use_cache: bool,
    use_unload: bool,
) -> None:
    """Execute one test mode repeatedly and print per-run details."""
    for run_index in range(1, runs + 1):
        result = _run_single(
            query=query,
            database=database,
            use_cache=use_cache,
            use_unload=use_unload,
        )
        print(
            f"{mode_name}\t{run_index}\t{result['elapsed_ms']}\t{result['execution_time_ms']}\t"
            f"{result['data_scanned_bytes']}\t{result['reused_previous_result']}\t"
            f"{result['row_count']}\t{result['athena_state']}\t{result['query_execution_id']}"
        )
        if pause_seconds > 0 and run_index < runs:
            time.sleep(pause_seconds)


def main() -> int:
    """Run cache probe in selected mode(s)."""
    args = _parse_args()
    query = _load_query(args)

    modes: list[tuple[str, bool]]
    if args.mode == "both":
        modes = [("no_cache", False), ("cache", True)]
    elif args.mode == "cache":
        modes = [("cache", True)]
    else:
        modes = [("no_cache", False)]

    _print_header()
    for mode_name, use_cache in modes:
        _run_mode(
            mode_name=mode_name,
            query=query,
            database=args.database,
            runs=args.runs,
            pause_seconds=args.pause_seconds,
            use_cache=use_cache,
            use_unload=args.use_unload,
        )

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
