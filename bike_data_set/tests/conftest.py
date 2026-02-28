from pyspark.sql import SparkSession
import pytest

@pytest.fixture(scope="session")
def spark():
    spark = SparkSession.builder.master("local[1]").appName("pytest-spark").getOrCreate()
    yield spark
    spark.stop()