"""
Thin read-only helpers that pull config from harris_county_catalog.config.
 
Every function returns plain Python structures (dicts / lists) so that
pipeline code stays decoupled from Spark DataFrames at the config layer.
 
Environment variables
---------------------
HARRIS_CATALOG  default: harris_county_catalog
HARRIS_SCHEMA   default: config
"""
 
from __future__ import annotations
 
import os
from typing import Any
 
from pyspark.sql import SparkSession, Row
 
# ---------------------------------------------------------------------------
# Coordinates
# ---------------------------------------------------------------------------
CATALOG = os.getenv("HARRIS_CATALOG", "harris_county_catalog")
SCHEMA  = os.getenv("HARRIS_SCHEMA",  "config")
FQN     = f"{CATALOG}.{SCHEMA}"
 
 
# ---------------------------------------------------------------------------
# Internal helper
# ---------------------------------------------------------------------------
def _query(spark: SparkSession, sql: str) -> list[Row]:
    return spark.sql(sql).collect()
 
 
# ---------------------------------------------------------------------------
# Public helpers
# ---------------------------------------------------------------------------
 
def get_column_renames(
    spark: SparkSession,
    pipeline_id: int,
    medallion: str,
    table_name: str,
) -> dict[str, str]:
    """
    Return {source_col: target_col} for the given pipeline / table.
 
    Used by silver layer to rename raw bronze columns before
    transformations are applied.
    """
    rows = _query(
        spark,
        f"""
        SELECT source_col, target_col
        FROM   {FQN}.pipeline_column_names
        WHERE  pipeline_id = {pipeline_id}
          AND  medallion   = '{medallion}'
          AND  table_name  = '{table_name}'
        """,
    )
    return {r.source_col: r.target_col for r in rows}
 
 
def get_transformations(
    spark: SparkSession,
    pipeline_id: int,
    medallion: str,
    table_name: str,
) -> list[dict[str, Any]]:
    """
    Return transformation steps ordered by step_order.
 
    Each step is a dict with keys:
        step_order, op, cols, val, extra
    Passed directly to the dispatcher in pipeline/transformations/dispatcher.py.
    """
    rows = _query(
        spark,
        f"""
        SELECT step_order, op, cols, val, extra
        FROM   {FQN}.pipeline_transformations
        WHERE  pipeline_id = '{pipeline_id}'
          AND  medallion   = '{medallion}'
          AND  table_name  = '{table_name}'
        ORDER  BY step_order
        """,
    )
    return [r.asDict() for r in rows]
 
 
def get_output_cols(
    spark: SparkSession,
    pipeline_id: int,
    medallion: str,
    table_name: str,
) -> list[str]:
    """
    Return the final column list in the correct output order.
 
    Used at the end of each silver / gold transformation to SELECT
    only the columns that belong in the target table.
    """
    rows = _query(
        spark,
        f"""
        SELECT col
        FROM   {FQN}.pipeline_output
        WHERE  pipeline_id = {pipeline_id}
          AND  medallion   = '{medallion}'
          AND  table_name  = '{table_name}'
        ORDER  BY col_order
        """,
    )
    return [r.col for r in rows]
 
 
def get_value_maps(
    spark: SparkSession,
    pipeline_id: int,
    medallion: str,
    table_name: str,
    col: str,
) -> dict[str, str]:
    """
    Return {source_val: target_val} for a value_map transformation step.
 
    Example: {'A1': 'single_family_residential', 'A2': 'mobile_home'}
    """
    rows = _query(
        spark,
        f"""
        SELECT source_val, target_val
        FROM   {FQN}.pipeline_value_maps
        WHERE  pipeline_id = {pipeline_id}
          AND  medallion   = '{medallion}'
          AND  table_name  = '{table_name}'
          AND  col         = '{col}'
        """,
    )
    return {r.source_val: r.target_val for r in rows}
 