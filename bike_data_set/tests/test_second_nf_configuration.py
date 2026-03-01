import pyspark.sql.types as T
from bike_data_project.transformations.silver_upload import second_nf_configuration

def test_second_nf_configuration(spark):
    spark = spark

    table_configs = {
        "primary_key": {"col1": 0}
    }

    test_cases = [
        {
            "name": "case_1",
            "data": [("11001", "")],
            "expected": [("11001", "")]
        },
        {
            "name": "case_2",
            "data": [
                ("11001", "placeholder_1"),
                ("11001", "placeholder_2"),
                ("11002", "")
            ],
            "expected": [("11002", "")]
        },
        {
            "name": "case_3",
            "data": [
                ("11001", ""),
                ("unknown", "")
            ],
            "expected": [("11001", "")]
        }
    ]

    schema = T.StructType([
        T.StructField("col1", T.StringType(), True),
        T.StructField("col2", T.StringType(), True)
    ])

    for case in test_cases:
        df = spark.createDataFrame(case["data"], schema)
        result_df = second_nf_configuration(df, table_configs)
        actual = [tuple(row) for row in result_df.collect()]

        assert actual == case["expected"], (
            f"Test failed for {case['name']}: "
            f"expected {case['expected']}, got {actual}"
        )























