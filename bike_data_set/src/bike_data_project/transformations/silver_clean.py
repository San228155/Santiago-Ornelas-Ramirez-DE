"""
Definitions of function used in the cleaning part of the Silver Transformations for the pipeline: triming and lowering, renaming, handling nulls and empty strings, handling hyphones, and casting
These functions requirements rarely change
"""

from pyspark.sql import functions as F
from pyspark.sql import DataFrame
from typing import Any
from pyspark.sql.types import IntegerType
from functools import reduce
from pyspark.sql.window import Window
from pyspark.sql.functions import xxhash64
from delta.tables import DeltaTable


def default_transformations(df: DataFrame, table_configs: dict[str, Any]) -> DataFrame:
    """
    Assumes all tables will need these transformations. Performes trim and lower on all tables and rows. Assumes all values are strings
    """
    if "defaults" in table_configs:
        df = df.select(*[
            F.lower(F.trim(F.col(c))).alias(c) for c in df.columns
        ])

    return df

def rename_columns(df: DataFrame, df_name: str, table_configs: dict[str, Any]) -> DataFrame:
    """
    Renames columns according to metadata
    """

    columns:dict[str, Any] = table_configs["columns"]

    select_expr = [
        F.col(col_val["source"]).alias(col_key) for col_key, col_val in columns.items()
    ]
    
    return df.select(
            *select_expr
        )

def handle_nulls_and_empty_strings(df: DataFrame, df_name: str, table_configs: dict[str, Any]) -> DataFrame:
    """
    Removes nulls by substituting them with a default value depending on the desired type of the column. {string: "unknown, int: "0", date: date format specified in metadata as a string}
    """

    string_cols: str = []
    int_cols: str = []
    date_cols: dict[str, list[str]] = {}

    for col_key, col_val in table_configs["columns"].items():
        cast_types = col_val.get("cast")
        if "cast" not in col_val:
            string_cols.append(col_key)
        elif "int" in cast_types:
            int_cols.append(col_key)
        elif "date" in cast_types:
            date_format = cast_types["default_value"]
            date_cols.setdefault(date_format, []).append(col_key)
        else:
            raise ValueError(f"Type {cast_types} not supported for column {col_key}")

    expr = []

    [
        expr.append(
            F.when(
                (F.col(c).isNull()) | (F.col(c) == ""), "unknown"
                )\
                .otherwise(F.col(c))\
                .alias(c)
            )
        for c in string_cols
    ]

    [
        expr.append(
            F.when(
                (F.col(c).isNull()) | (F.col(c) == ""), "0"
                )\
                .otherwise(F.col(c))\
                .alias(c)
            )
        for c in int_cols
    ]

    for date_format, c in date_cols.items():
        for col in c:
            expr.append(
            F.when(
                (F.col(col).isNull()) | (F.col(col) == ""), date_format
                )\
                .otherwise(F.col(col))\
                .alias(col)
            )

    return df.select(
        *expr
    )

def handle_hyphon(df: DataFrame, df_name: str, table_configs: dict[str, Any]) -> DataFrame:
    """
    Removes or replaces hyphones. If replaced, makes them an underscore
    """
    expr = []

    for col_key, col_val in table_configs["columns"].items():
        if col_val.get("replace_hyphon", False): # regex replaces each consecutive run of spaces and hyphones into an underscore
            expr.append(
                F.regexp_replace(
                    F.col(col_key),
                    r"[\s\-]+",
                    "_"
                ).alias(col_key)
            )
        elif col_val.get("remove_hyphon"):
            expr.append(
                F.regexp_replace(F.col(col_key), "-", "").alias(col_key)
            )

        else:
            expr.append(F.col(col_key))
    return df.select(*expr)
    

def casting(df: DataFrame, df_name: str, table_configs:dict[str, Any]) -> DataFrame:
    """
    Cast columns based on information in metadata
    """

    expr = []

    for col_key, col_val in table_configs["columns"].items():
        
        if not col_val.get("cast"):
            expr.append(F.col(col_key))
        
        elif "int" in col_val["cast"]:
            expr.append(F.col(col_key).cast("int").alias(col_key))
        
        elif "date" in col_val["cast"]:
            expr.append(F.try_to_date(F.col(col_key), col_val["cast"]["date"]).alias(col_key))

    return df.select(
        *expr
    )
