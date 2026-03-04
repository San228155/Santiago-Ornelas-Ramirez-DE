import logging

from pyspark.sql import SparkSession

from bike_data_project.config.customer_gold_metadata import (
    customer_gold_metadata,
    product_gold_metadata,
    sales_gold_meta_data,
)
from bike_data_project.runner.gold_runner import gold_table_upload

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def get_spark():
    spark = SparkSession.getActiveSession()
    if spark is None:
        spark = SparkSession.builder.getOrCreate()
    return spark

def main():
    input_path = "bike_data_lakehouse.silver"
    output_path = "bike_data_lakehouse.gold"
    spark = get_spark()

    gold_table_metadata = [customer_gold_metadata, product_gold_metadata, sales_gold_meta_data]

    for table_metadata in gold_table_metadata:
        logger.info(f"creating table {table_metadata}")
        gold_table_upload(spark, table_metadata, input_path, output_path)

if __name__ == "__main__":
    main()






