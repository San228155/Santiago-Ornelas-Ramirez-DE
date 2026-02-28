from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T
from pyspark.sql import DataFrame
from typing import Any
from functools import reduce
from src.bike_data_project.transformations.silver_preprocess import apply_enum_aliases

# def apply_enum_aliases(df: DataFrame, table_configs: dict[str, any]) -> DataFrame:
#     """
#     Applies mappings to make abbreviations and aliases into a standard name
#     Validates all values are in an acceptable list (enum)
#     """

#     exprs = []
#     quarantine_dfs = []
#     validation_conditions = []

#     for col_key, col_val in table_configs["columns"].items():
#         # we use a when condition chain to express the mappings
#         # We do not use a join or broadcast join as the number of possible values is small < 20
#         if "map" in col_val:
#             when_condition_chain = None

#             for desired_val, possible_vals in col_val["map"].items():
#                 cond = F.col(col_key).isin(possible_vals)
#                 when_condition_chain = F.when(cond, F.lit(desired_val)) if when_condition_chain is None else when_condition_chain.when(cond, F.lit(desired_val))

#             normalized_col = when_condition_chain.otherwise(F.col(col_key))  

#         else:
#             normalized_col = F.col(col_key)

#         exprs.append(normalized_col.alias(col_key))

#         # optional check to ensure only allowed values are in the column
#         if "validation" in col_val and "enum" in col_val["validation"]:
#             valid_values = col_val["validation"]["enum"]
            
#             validation_conditions.append(normalized_col.isin(valid_values))

#     if validation_conditions:
#         filter_condition = reduce(lambda x,y: x & y, validation_conditions)

#     normalized_df = df.select(*exprs)

#     normalized_df = normalized_df.withColumn(
#         "is_valid",
#         filter_condition
#     )

#     valid_df = normalized_df.filter(F.col("is_valid"))
#     quarantine_df = normalized_df.filter(~F.col("is_valid"))

#     return valid_df

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
        "columns": {
            "col1": {
                "map": {
                "first" : ["1st_map"], 
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
            "name": "test_case",
            "data": [
                ("1st_map", "this value should not change"),
                ("second", "this value should not change"),
                ("third", "this row should not appear")
            ],
            "expected": [("first","1 should not be mapped", True), ("second", "2 should not be mapped", True)]
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

        if actual != case["expected"]:
            raise ValueError(
                f"Test failed for {case['name']}: "
                f"expected {case['expected']}, got {actual}"
            )

        print(f"Test passed for {case['name']}")


if __name__ == "__main__":
    main()


























