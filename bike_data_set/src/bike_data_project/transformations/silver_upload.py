"""
Definition of functions for Silver Transformations: surrogate key addition, second nf configuration, and creating the table
Surrogate key addition uses all tables to create a unified surrogate key definition
These logic behind the tables rarely changes
"""

from pyspark.sql import functions as F
from pyspark.sql import DataFrame
from typing import Any
from functools import reduce
from pyspark.sql.functions import xxhash64
from delta.tables import DeltaTable


def surrogate_key_addition(spark, meta_data)->None:
    """
    Creates a "master" table of surrogate keys
    Joins tables that share a key, creates the surrogate key using hash64 and redistributes them into the tables
    """
    intelligent_tables = {}
    surrogate_key_dict = {}

    ingestion_root_path = "bike_data_lakehouse.silver_preprocessed."
    out_path = "bike_data_lakehouse.silver_surrogate."

    # creates a dictionary which details which tables need which surrogate key
    table_names = [names for names in meta_data]
    for t_names in table_names:
        table_path = f"{ingestion_root_path}{t_names}"
        df = spark.read.table(f"{table_path}")
        intelligent_tables[t_names] = df

    for table_name in intelligent_tables:
        surrogate_config = list(meta_data[table_name]["surrogate_key"].keys())
        if "dim_customer_sk" in surrogate_config:
            surrogate_key_dict.setdefault("dim_customer_sk", []).append(table_name)
        
        if "dim_product_sk" in surrogate_config:
            surrogate_key_dict.setdefault("dim_product_sk", []).append(table_name)

        if "dim_category_sk" in surrogate_config:
            surrogate_key_dict.setdefault("dim_category_sk", []).append(table_name)

    # creates table of only surrogate keys
    for key, tables in surrogate_key_dict.items():

        # drop_logic = f"DROP TABLE IF EXISTS bike_data_lakehouse.surrogate_keys.surrogate_key_{key}_table"

        # spark.sql(drop_logic)
        column_name = meta_data[tables[0]]["surrogate_key"][key]

        create_table_ddl = f"""
            CREATE TABLE IF NOT EXISTS bike_data_lakehouse.surrogate_keys.surrogate_key_{key}_table (
                {column_name} STRING NOT NULL,
                {key} LONG NOT NULL
            )
        """

        spark.sql(create_table_ddl)

        dfs = [
            intelligent_tables[t].select(column_name)
            for t in tables
        ]

        dimension_df = reduce(
            lambda df1, df2: df1.union(df2),
            dfs
        )

        dimension_df = (
            dimension_df
            .select(F.col(column_name))
            .distinct()
        )


        df_with_sk = dimension_df.withColumn(
            key,
            xxhash64(F.col(column_name))
        )

        delta_target = DeltaTable.forName(spark, f"bike_data_lakehouse.surrogate_keys.surrogate_key_{key}_table") #returns a delta table not a dataFrame, allows upload, merge, etc

        (
        delta_target.alias("t")
        .merge(
            df_with_sk.alias("s"),
            f"t.{column_name} = s.{column_name}"
        )
        .whenNotMatchedInsertAll()
        .execute()
        )
    
    # redistributre the surrogate key to the corresponding tables

    sk_dict = {}
    sk_tables_sql = "SHOW TABLES IN bike_data_lakehouse.surrogate_keys"
    sk_tables = [row.tableName for row in spark.sql(sk_tables_sql).collect()]
    
    for table in sk_tables:
        sk_dict[table] = spark.read.table(f"bike_data_lakehouse.surrogate_keys.{table}")

    for table_name, data_frame in intelligent_tables.items():
        for surrogate_key, column_name in meta_data[table_name]["surrogate_key"].items():
            sk_join_table = sk_dict[f"surrogate_key_{surrogate_key}_table"]
            data_frame = data_frame.join(
                F.broadcast(sk_join_table),
                on=[column_name],
                how="left"
            )
        data_frame.write.mode("overwrite").saveAsTable(f"{out_path}{table_name}")

def second_nf_configuration(df: DataFrame, table_configs: dict[str, Any]) -> DataFrame:
    """
    Separates df into 2nd NF compliant rows and quarantines non compliant rows
    """

    primary_key = list(table_configs["primary_key"].keys())
    
    pk_unknown_conditions = [
        F.col(col) == F.lit("unknown") for col in primary_key
    ]

    unknown_condition = reduce(lambda a, b: a | b, pk_unknown_conditions)

    malformed_condition = (F.col("duplicate_rows") > 1) | unknown_condition

    malformed_rows = (
        df.groupBy(*primary_key)
        .agg(F.count("*").alias("duplicate_rows"))
        .filter(malformed_condition)
    )

    if malformed_rows.limit(1).count() != 0:
        duplicate_keys = [
            tuple(row[col] for col in primary_key)
            for row in malformed_rows.collect()
        ]

        duplicate_key_structs = [
            F.struct(*[F.lit(v) for v in key_tuple])
            for key_tuple in duplicate_keys
        ]


        # quarantine_df = df.join(duplicate_rows, on=primary_key, how="inner")

        second_nf_compliant_rows = df.filter(
            ~F.struct(*primary_key).isin(duplicate_key_structs)
        )

        # upload to a quarantine
        return second_nf_compliant_rows
    return df

def create_table(df: DataFrame, df_name: str, table_configs: dict[str, Any], spark) -> None:
    """
    Writes table to delta lake and enforces data quality (Unique and not null)
    Assigns primary keys and foreign keys
    """
    
    catalog = "bike_data_lakehouse"
    schema = "silver"

    drop_logic = f"DROP TABLE IF EXISTS bike_data_lakehouse.silver.{df_name}"

    spark.sql(drop_logic)

    primary_key = table_configs["primary_key"]

    cast_types = {"date", "int"}

    expr = []
    table_constraints = []
    composite_pk_cols = []

    is_composite_pk = len(primary_key) > 1

    for col_name, col_val in table_configs["columns"].items():

        cast_info = col_val.get("cast", {})
        cast_type = next((k for k in cast_info if k in cast_types), None)
        dtype = cast_type.upper() if cast_type else "STRING"

        if col_name in primary_key:
            length_of_pk = primary_key[col_name]
            if is_composite_pk:
                expr.append(
                    f"{col_name} {dtype} NOT NULL "
                    f"CHECK (LENGTH({col_name}) = {length_of_pk})"
                )
                composite_pk_cols.append(col_name)
            else:
                expr.append(
                    f"{col_name} {dtype} NOT NULL PRIMARY KEY UNIQUE "
                    f"CHECK (LENGTH({col_name}) = {length_of_pk})"
                )
        else:
            expr.append(f"{col_name} {dtype}")

    if is_composite_pk:
        cols = ", ".join(composite_pk_cols)
        table_constraints.append(
            f"CONSTRAINT pk_{df_name} PRIMARY KEY ({cols})"
        )
        table_constraints.append(
            f"CONSTRAINT uq_{df_name} UNIQUE ({cols})"
        )

    column_expr = ", ".join(expr + table_constraints)

    create_table_sql = f"""
        CREATE TABLE IF NOT EXISTS {catalog}.{schema}.{df_name} (
            {column_expr}
        )
    """

    show_tables = f"SHOW TABLES IN {catalog}.{schema}"
    existing_tables = spark.sql(show_tables).collect()
    existing_table_names = {row.tableName for row in existing_tables}

    if df_name not in existing_table_names:
        spark.sql(create_table_sql)
        df.write.mode("append").saveAsTable(f"{catalog}.{schema}.{df_name}")

    else:
        print(f"ERRROR - failed to write: this table {df_name} already exists in schema")

    


