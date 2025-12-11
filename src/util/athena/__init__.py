"""Helpers for interacting with AWS Athena and S3."""

from .athena import (
    query_to_dataframe,
    aoi_query_to_dataframe,
    query_to_local_parquet,
    aoi_query_to_local_parquet,
    get_field_metadata,
    # legacy
    athena_select_to_dict,
    athena_select_to_dataframe,
    athena_unload_to_dict,
    athena_unload_to_dataframe,
    get_data_for_aoi,
    run_aoi_athena_query,
)
__all__ = [
    # preferred
    "query_to_dataframe",
    "aoi_query_to_dataframe",
    "query_to_local_parquet",
    "aoi_query_to_local_parquet",
    "get_field_metadata",
    # legacy
    "athena_select_to_dict",
    "athena_select_to_dataframe",
    "athena_unload_to_dict",
    "athena_unload_to_dataframe",
    "get_data_for_aoi",
    "run_aoi_athena_query",
]
