from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T
from pyspark.sql import DataFrame
from typing import Any
from src.bike_data_project.transformations.silver_clean import default_transformations

# def default_transformations(df: DataFrame, table_configs: dict[str, Any]) -> DataFrame:
#     """
#     Assumes all tables will need these transformations. Performes trim and lower on all tables and rows. Assumes all values are strings
#     """
#     if "defaults" in table_configs:
#         df = df.select(*[
#             F.lower(F.trim(F.col(c))).alias(c) for c in df.columns
#         ])

#     return df


def get_spark():
    spark = SparkSession.getActiveSession()
    if spark is None:
        spark = SparkSession.builder.getOrCreate()
    return spark


def main():
    """
    test default transformation from silver_clean.py
    """
    spark = get_spark()

    table_configs = {
        "defaults": ["trim", "lower"]
    }

    test_cases = [
        {
            "name": "control_trim_upper_test",
            "data": [
                ("control_sample",),
                ("   trim_test   ",),
                ("Upper_Case_Test",)
            ],
            "expected": ["control_sample", "trim test", "upper_case_test"]
        }
    ]
    schema = T.StructType([
        T.StructField("col1", T.StringType(), True)
    ])

    for case in test_cases:
        df = spark.createDataFrame(case["data"], schema)

        result_df = default_transformations(df, table_configs)

        actual = [row[0] for row in result_df.collect()]

        if actual != case["expected"]:
            raise ValueError(
                f"Test failed for {case['name']}: "
                f"expected {case['expected']}, got {actual}"
            )

        print(f"Test passed for {case['name']}")


if __name__ == "__main__":
    main()


