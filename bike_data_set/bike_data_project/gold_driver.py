import sys
sys.path.append('/Workspace/Santiago-Ornelas-Ramirez/bike_data_set/bike_data_project/')
from pyspark.sql import SparkSession

from config.customer_gold_metadata import (
    customer_gold_metadata,
    product_gold_metadata,
    sales_gold_meta_data
)

from runner.gold_runner import (
    gold_table_upload
)

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
        gold_table_upload(spark, table_metadata, input_path, output_path)


if __name__ == "__main__":
    main()






