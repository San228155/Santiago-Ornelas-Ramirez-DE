"""
Contains a function that orchestrates all necessary functions for the bronze step in the pipeline. Python definitions can be found in bronze.py
"""
from pyspark.sql import DataFrame
from typing import Any

from bike_data_project.transformations.bronze import (
    missing_configs,
    overwrite_allowed,
    valid_file_type,
    path_checker,
    is_table_empty
)

def ingestion(spark, catalog: str, ingestion_schema: str, output_schema:str, volume:str, clean_table_name:str, config_dict:dict[str, Any]) -> None: #should not be importing last 4 arugmetns, should be read from configs and then extracted through read meta data
    missing_configs(config_dict=config_dict)

    original_table_name: str = config_dict["source"]
    # if the file is not allowed to be overwritten, we assume the file has passed the necessary filters and does not need to be replaced
    if overwrite_allowed(original_table_name=original_table_name, config_dict=config_dict):
        return None
    
    valid_file_type(original_table_name=original_table_name, config_dict=config_dict)

    print(f"Ingesting {original_table_name}")

    path: str = f"/Volumes/{catalog}/{ingestion_schema}/{volume}/{original_table_name}.csv"
    
    if not path_checker(path):
        raise ValueError(f"File not found: {path}")

    # we expect all columns to be strings, hence we dont use inferSchema
    reader = spark.read.option("header", "true")
    df:DataFrame = reader.csv(path)

    if is_table_empty(spark, df=df):
        raise ValueError(f"uploaded table is empty: {catalog}.{output_schema}.{clean_table_name}")

    data_frame_columns = df.columns
    print(f"columns of table {original_table_name}: {data_frame_columns}")

    complete_write_path = f"{catalog}.{output_schema}.{clean_table_name}"
    df.write.mode("overwrite").format("delta").saveAsTable(complete_write_path)
        # we check the row count to ensure that the table was read correctly, else we want the program to raise an error
