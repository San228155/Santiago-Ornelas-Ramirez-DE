import sys
from config.meta_driven_transformations import TRANSFORMATION
from pyspark.sql import SparkSession

from transformations.silver_clean import (
    clean_data_tables
)

from transformations.silver_preprocessed import (
    preprocess_data_tables
)

from transformations.silver_upload import (
    silver_table_surrogate, silver_table_upload
)

def get_spark():
    spark = SparkSession.getActiveSession()
    if spark is None:
        spark = SparkSession.builder.getOrCreate()
    return spark

def main():
    sys.path.append('/Workspace/Santiago-Ornelas-Ramirez/bike_data_set/bike_data_project/')
    catalog = "bike_data_lakehouse"
    ingestion_schema = "raw_data"
    output_schema = "bronze"
    spark = get_spark()

    for table_name, table_config in TRANSFORMATION.items():
        clean_data = clean_data_tables(spark, table_name, table_config)
    for table_name, table_config in TRANSFORMATION.items():
        preprocessed_tables = preprocess_data_tables(spark=spark, table_name=table_name, table_config=table_config)
        silver_table_surrogate(spark=spark, TRANSFORMATION=TRANSFORMATION)
    for table_name, table_config in TRANSFORMATION.items():
        silver_table_upload(spark=spark, table_name=table_name, table_config=table_config)


if __name__ == "__main__":
    main()








