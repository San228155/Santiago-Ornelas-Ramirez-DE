from typing import Any

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def aggregation(spark, table_metadata:dict[str, Any], input_path:str, ) -> DataFrame:
    """
    Aggregation on table type (customers, products, and sales)
    """
    silver_table_names:list[dict[str, str]] = [t for t in table_metadata["tables"]]
    select_expr = []
    base_df: DataFrame = None
    df_list_excluding_first: list[DataFrame] = []
    coalesce_columns: set[str] = table_metadata.get("coalesce_columns", {})

    # get the join information for the join
    if table_metadata.get("join_key"):
        join_key = table_metadata["join_key"]
        join_type = table_metadata["join_type"]

    # read the tables
    for i, silver_df in enumerate(silver_table_names):
        df_path: str = f"{input_path}.{silver_df}"
        if i == 0:
            base_df = spark.read.table(df_path)
        else:
            df_list_excluding_first.append(spark.read.table(df_path))

    # join the tables to the base dataframe
    for tables_to_be_joined in df_list_excluding_first:
        base_df = base_df\
        .join(
            tables_to_be_joined, 
            on=join_key, 
            how=join_type
        )
  
    # make a select expression for all the gold table with coalesce logic for columns repeated in various tables
    for col_name in coalesce_columns:
        cols_to_coalesce = []
        for tb_name, columns in table_metadata["tables"].items():
            if col_name in columns:
                cols_to_coalesce.append(
                    F.col(f"{tb_name}.{col_name}")
                )

        select_expr.append(
            F.coalesce(*cols_to_coalesce).alias(col_name)
        )

    for tb_name, columns in table_metadata["tables"].items():
        for col in columns:
            if col not in coalesce_columns:
                select_expr.append(
                    F.col(f"{tb_name}.{col}")
                )
    
    return base_df.select(*select_expr)



































