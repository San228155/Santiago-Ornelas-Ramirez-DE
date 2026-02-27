import sys
from typing import Any
from pyspark.sql import DataFrame

sys.path.append('/Workspace/Santiago-Ornelas-Ramirez/bike_data_set/bike_data_project/')

from transformations.gold import (
    aggregation
)

def gold_table_upload(spark, table_metadata, input_path, output_path):
    df = aggregation(spark, table_metadata=table_metadata, input_path=input_path)
    write_path = f"{output_path}.{table_metadata["name"]}"
    df.write.mode("overwrite").saveAsTable(write_path)




