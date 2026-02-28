from typing import Any
from pyspark.sql import DataFrame

from bike_data_project.transformations.silver_clean import (
    default_transformations,
    rename_columns,
    handle_nulls_and_empty_strings,
    handle_hyphon,
    casting
)

from bike_data_project.transformations.silver_preprocess import (
    apply_enum_aliases,
    data_augmentation,
    intelligent_key_preparation
)

from bike_data_project.transformations.silver_upload import (
    surrogate_key_addition,
    second_nf_configuration,
    create_table
)

def clean_data_tables(spark, table_name:str, table_config:dict[str, Any]) -> None:
    print("cleaning_data")
    incoming_schema = "bike_data_lakehouse.bronze."
    old_table_name = table_config["source"]

    out_schema = "bike_data_lakehouse.silver_clean."

    incoming_table = f"{incoming_schema}{old_table_name}" 
    df = spark.read.table(f"{incoming_table}")

    df = default_transformations(df, table_config) 
    df = rename_columns(df, table_name, table_config)
    df = handle_nulls_and_empty_strings(df, table_name, table_config)
    df = handle_hyphon(df, table_name, table_config)
    df = casting(df, table_name, table_config)

    silver_table_name = f"{out_schema}{table_name}"
    df.write.mode("overwrite").saveAsTable(f"{silver_table_name}")

def preprocess_data_tables(spark, table_name:str, table_config:dict[str, Any]) -> None:
    print("preprocessing data")
    incoming_schema = "bike_data_lakehouse.silver_clean."
    old_table_name = table_config["source"]

    out_schema = "bike_data_lakehouse.silver_preprocessed."
    
    df = spark.read.table(f"{incoming_schema}{table_name}")

    df = apply_enum_aliases(df, table_config)
    df = data_augmentation(df, table_name, table_config)
    df = intelligent_key_preparation(df, table_name, table_config)

    df.write.mode("overwrite").saveAsTable(f"{out_schema}{table_name}")

def silver_table_surrogate(spark, TRANSFORMATION:dict[str,Any]):
    print("surrogate transformation")
    surrogate_tables = surrogate_key_addition(spark, TRANSFORMATION)

def silver_table_upload(spark, table_name:str, table_config:dict[str, Any]) -> None:
    print("uploading")
    incoming_schema = "bike_data_lakehouse.silver_surrogate."
    old_table_name = table_config["source"]

    out_schema = "bike_data_lakehouse.silver."
    
    df = spark.read.table(f"{incoming_schema}{table_name}")
    normalized_tables = second_nf_configuration(df, table_config)
    create_table(df, table_name, table_config, spark)

















