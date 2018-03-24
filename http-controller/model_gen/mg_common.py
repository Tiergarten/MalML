from pyspark.sql.session import SparkSession
from pyspark import SparkContext


def get_df(data):
    return SparkSession.builder.getOrCreate().createDataFrame(data)


def get_sc():
    return SparkContext("local", "static-poc")
