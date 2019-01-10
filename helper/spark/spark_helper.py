"""
Class Spark Helper

@author Moch Nurhalimi Zaini D <moch.nurhalimi@gmail.com>
@author Irfan Andriansyah <irfan@99.co>
"""
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType


class SparkHelper:
    """
    Helper for spark
    """

    @staticmethod
    def create_connection():
        """Create connection spark.

        Usage
        SparkHelper.create_connection()
        """

        return SparkSession.builder.appName(
            "Spark for ETL data 99.co ID"
        ).config("spark.some.config.option", "some-value").getOrCreate()

    @staticmethod
    def read_parquet(spark, config):
        """Create parquet based on config parameter

        Usage
        SparkHelper.read_parquet()

        :param spark: (Object) Spark Session.
        :param config: (String) Directory path data frame.
        """
        try:
            if spark is not None:
                return spark.read.parquet(config)

            raise Exception('Spark parameter is None !!')
        except Exception as error:
            raise Exception(error)

    @staticmethod
    def read_json(spark, config):
        """Create read data from csv based on config parameter

        Usage
        SparkHelper.read_parquet()

        :param spark: (Object) Spark Session.
        :param config: (String) Directory path data frame.
        """
        try:
            if spark is not None:
                return spark.read.json(config)

            raise Exception('Spark parameter is None !!')
        except Exception as error:
            raise Exception(error)

    @staticmethod
    def get_udf(key, parse):
        """
        Get User definition function for transform data search tracker

        Usage
        SparkHelper.readParquet()

        :param key: (String) key object.
        :param parse: (Function) Closure callback get_udf method.
        """
        return udf(lambda params: parse(key, params), StringType())
