from pyspark.sql import SparkSession

from bike_data_project.config.meta_driven_ingestion import INGESTION
from bike_data_project.runner.bronze_runner import ingestion

catalog = "bike_data_lakehouse"
ingestion_schema = "raw_data"
output_schema = "bronze"

def get_spark():
    spark = SparkSession.getActiveSession()
    if spark is None:
        spark = SparkSession.builder.getOrCreate()
    return spark

def main():
    spark = get_spark()

    for volume, table_dicts in INGESTION.items():
        for clean_table_name, config_dict in table_dicts.items():
            ingestion(spark= spark, catalog = catalog, ingestion_schema= ingestion_schema, output_schema= output_schema, volume=volume, clean_table_name=clean_table_name, config_dict=config_dict)

if __name__ == "__main__":
    main()