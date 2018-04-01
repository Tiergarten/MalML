from pyspark.sql.session import SparkSession
from pyspark import SparkContext

from elasticsearch import Elasticsearch
from elasticsearch_dsl import Search

def get_elastic():
    return Elasticsearch()

def get_df(data):
    return SparkSession.builder.getOrCreate().createDataFrame(data)


def get_sc():
    return SparkContext("local", "static-poc")



