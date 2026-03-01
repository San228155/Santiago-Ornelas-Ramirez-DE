from pyspark.sql import types as T

from bike_data_project.transformations.bronze import is_table_empty


def test_is_table_empty(spark):
    """
    Test is_table_empty function from bronze.py
    """

    test_cases = [
        {
            "name": "empty_table",
            "data": [],
            "expected": True
        },
        {
            "name": "normal_table",
            "data": [("value",)],
            "expected": False
        }
    ]

    schema = T.StructType([
        T.StructField("col1", T.StringType(), True)
    ])

    for case in test_cases:
        df = spark.createDataFrame(case["data"], schema)

        result = is_table_empty(spark, df)

        assert result == case["expected"], (
            f"Test failed for {case['name']}: "
            f"expected {case['expected']}, got {result}"
        )





