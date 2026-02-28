from pyspark.sql import types as T
from bike_project.transformations.silver_preprocess import apply_enum_aliases


def test_apply_enum_aliases(spark):
    """
    Test apply_enum_aliases from silver_preprocess.py
    """

    table_configs = {
        "columns": {
            "col1": {
                "map": {
                    "first": ["1st_map"],
                    "second": ["2"],
                },
                "validation": {
                    "enum": ["first", "second"],
                    "enforce": True
                }
            },
            "col2": {}
        }
    }

    test_cases = [
        {
            "name": "enum_mapping_and_validation",
            "data": [
                ("1st_map", "this value should not change"),
                ("second", "this value should not change"),
                ("third", "this row should not appear")
            ],

            "expected": [
                ("first", "this value should not change"),
                ("second", "this value should not change"),
            ]
        }
    ]

    schema = T.StructType([
        T.StructField("col1", T.StringType(), True),
        T.StructField("col2", T.StringType(), True)
    ])

    for case in test_cases:
        df = spark.createDataFrame(case["data"], schema)

        result_df = apply_enum_aliases(df, table_configs)

        actual = [tuple(row) for row in result_df.collect()]

        assert actual == case["expected"], (
            f"Test failed for {case['name']}: "
            f"expected {case['expected']}, got {actual}"
        )


























