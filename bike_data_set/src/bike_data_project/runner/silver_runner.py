from typing import Any

from pyspark.sql import DataFrame

from bike_data_project.transformations.silver_clean import (
    casting,
    default_transformations,
    handle_hyphon,
    handle_nulls_and_empty_strings,
    rename_columns,
)
from bike_data_project.transformations.silver_preprocess import (
    apply_enum_aliases,
    data_augmentation,
    intelligent_key_preparation,
)
from bike_data_project.transformations.silver_upload import (
    create_table,
    second_nf_configuration,
    surrogate_key_addition,
)


def clean_data_tables(spark, table_name:str, table_config:dict[str, Any]) -> None:
    print("cleaning_data")
    incoming_schema:str = "bike_data_lakehouse.bronze."
    old_table_name:str = table_config["source"]

    out_schema:str = "bike_data_lakehouse.silver_clean."

    incoming_table:str = f"{incoming_schema}{old_table_name}" 
    df:DataFrame = spark.read.table(f"{incoming_table}")

    df:DataFrame = default_transformations(df, table_config) 
    df:DataFrame = rename_columns(df, table_name, table_config)
    df:DataFrame = handle_nulls_and_empty_strings(df, table_name, table_config)
    df:DataFrame = handle_hyphon(df, table_name, table_config)
    df:DataFrame = casting(df, table_name, table_config)

    silver_table_name:str = f"{out_schema}{table_name}"
    df.write.mode("overwrite").saveAsTable(f"{silver_table_name}")

def preprocess_data_tables(spark, table_name:str, table_config:dict[str, Any]) -> None:
    print("preprocessing data")
    incoming_schema = "bike_data_lakehouse.silver_clean."

    out_schema = "bike_data_lakehouse.silver_preprocessed."
    
    df = spark.read.table(f"{incoming_schema}{table_name}")

    df = apply_enum_aliases(df, table_config)
    df = data_augmentation(df, table_name, table_config)
    df = intelligent_key_preparation(df, table_name, table_config)

    df.write.mode("overwrite").saveAsTable(f"{out_schema}{table_name}")

def silver_table_surrogate(spark, TRANSFORMATION:dict[str,Any]):
    print("surrogate transformation")
    surrogate_key_addition(spark, TRANSFORMATION)

def silver_table_upload(spark, table_name:str, table_config:dict[str, Any]) -> None:
    print("uploading")
    incoming_schema = "bike_data_lakehouse.silver_surrogate."

    df = spark.read.table(f"{incoming_schema}{table_name}")
    normalized_tables = second_nf_configuration(df, table_config)
    create_table(normalized_tables, table_name, table_config, spark)

















