from pyspark.sql import types as T

from bike_data_project.transformations.silver_clean import default_transformations


def test_default_transformations(spark):
    """
    Test default_transformations from silver_clean.py
    """

    table_configs = {
        "defaults": ["trim", "lower"]
    }

    test_cases = [
        {
            "name": "control_trim_lower_test",
            "data": [
                ("control_sample",),
                ("   trim_test   ",),
                ("Upper_Case_Test",)
            ],
            "expected": [
                "control_sample",
                "trim_test",
                "upper_case_test"
            ]
        }
    ]

    schema = T.StructType([
        T.StructField("col1", T.StringType(), True)
    ])

    for case in test_cases:
        df = spark.createDataFrame(case["data"], schema)

        result_df = default_transformations(df, table_configs)

        actual = [row[0] for row in result_df.collect()]

        assert actual == case["expected"], (
            f"Test failed for {case['name']}: "
            f"expected {case['expected']}, got {actual}"
        )


