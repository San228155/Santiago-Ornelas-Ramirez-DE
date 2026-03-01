from typing import Any

from pyspark.sql import DataFrame

from bike_data_project.transformations.gold import (
    aggregation,
)

def gold_table_upload(spark, table_metadata: dict[str, Any], input_path:str, output_path:str):
    df:DataFrame = aggregation(spark, table_metadata=table_metadata, input_path=input_path)
    write_path:str = f"{output_path}.{table_metadata['name']}"
    df.write.mode("overwrite").saveAsTable(write_path)




