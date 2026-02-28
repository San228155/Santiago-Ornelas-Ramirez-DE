from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T
from pyspark.sql import DataFrame
from typing import Any
from functools import reduce
from src.bike_data_project.transformations.silver_upload import second_nf_configuration


# def second_nf_configuration(df: DataFrame, table_configs: dict[str, Any]) -> None:
#     """
#     Separates df into 2nd NF compliant rows and quarantines non compliant rows
#     """

#     primary_key = list(table_configs["primary_key"].keys())
    
#     pk_unknown_conditions = [
#         F.col(col) == F.lit("unknown") for col in primary_key
#     ]

#     unknown_condition = reduce(lambda a, b: a | b, pk_unknown_conditions)

#     malformed_condition = (F.col("duplicate_rows") > 1) | unknown_condition

#     malformed_rows = (
#         df.groupBy(*primary_key)
#         .agg(F.count("*").alias("duplicate_rows"))
#         .filter(malformed_condition)
#     )

#     if malformed_rows.limit(1).count() != 0:
#         duplicate_keys = [
#             tuple(row[col] for col in primary_key)
#             for row in malformed_rows.collect()
#         ]

#         duplicate_key_structs = [
#             F.struct(*[F.lit(v) for v in key_tuple])
#             for key_tuple in duplicate_keys
#         ]


#         # quarantine_df = df.join(duplicate_rows, on=primary_key, how="inner")

#         second_nf_compliant_rows = df.filter(
#             ~F.struct(*primary_key).isin(duplicate_key_structs)
#         )

#         # upload to a quarantine
#         return second_nf_compliant_rows
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
        "primary_key": 
            {"col1": 0}
    }

    test_cases = [
        {
            "name": "test_case",
            "data": [
                ("11001","")
            ],
            "expected": [("11001","")]
        },
        {
            "name": "test_case",
            "data": [
                ("11001","placeholder_1"),
                ("11001","placeholder_2"),
                ("11002","")
            ],
            "expected": [("11002","")]
        },
        {
            "name": "test_case",
            "data": [
                ("11001",""),
                ("unknown","")
            ],
            "expected": [("11001","")]
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

        if actual != case["expected"]:
            raise ValueError(
                f"Test failed for {case['name']}: "
                f"expected {case['expected']}, got {actual}"
            )

        print(f"Test passed for {case['name']}")


if __name__ == "__main__":
    main()























