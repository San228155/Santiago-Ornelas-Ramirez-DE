from pyspark.sql import SparkSession
from pyspark.sql import types as T
from src.bike_data_project.transformations.bronze import is_table_empty
    
def get_spark():
    spark = SparkSession.getActiveSession()
    if spark is None:
        spark = SparkSession.builder.getOrCreate()
    return spark


def main():
    """
    test is_table_empty function from bronze.py
    """
    spark = get_spark()

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

        if result != case["expected"]:
            raise ValueError(f"Test failed for {case['name']}: expected {case['expected']}, got {result}")

        print(f"Test passed for {case['name']}")


if __name__ == "__main__":
    main()






