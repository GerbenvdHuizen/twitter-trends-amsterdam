from pyspark.sql import SparkSession
from pytest import fixture


@fixture(scope="session")
def spark() -> SparkSession:
    return SparkSession.builder.getOrCreate()
