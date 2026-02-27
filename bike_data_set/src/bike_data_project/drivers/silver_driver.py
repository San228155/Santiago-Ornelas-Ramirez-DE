from pyspark.sql import SparkSession

from bike_data_project.config.meta_driven_transformations import (
    TRANSFORMATION
)


from bike_data_project.transformations.silver_clean import (
    clean_data_tables
)

from bike_data_project.transformations.silver_preprocessed import (
    preprocess_data_tables
)

from bike_data_project.transformations.silver_upload import (
    silver_table_surrogate, silver_table_upload
)

def get_spark():
    spark = SparkSession.getActiveSession()
    if spark is None:
        spark = SparkSession.builder.getOrCreate()
    return spark

def main():
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








