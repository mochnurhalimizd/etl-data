"""
Class Spark Helper

@author Moch Nurhalimi Zaini D <moch.nurhalimi@gmail.com>
@author Irfan Andriansyah <irfan@99.co>
"""
from pyspark.sql import SparkSession


class SparkHelper:
    """
    Helper for spark
    """

    @staticmethod
    def create_connection():
        """Create connection spark.

        Usage
        SparkHelper.createConnection()
        """

        return SparkSession.builder.appName(
            "Spark for ETL data 99.co ID"
        ).config("spark.some.config.option", "some-value").getOrCreate()

    @staticmethod
    def read_parquet(spark, config):
        """Create parquet based on config parameter

        Usage
        SparkHelper.readParquet()

        :param spark: (Object) Spark Session.
        :param config: (String) Directory path data frame.
        """
        try:
            if spark is not None:
                return spark.read.parquet(config)

            raise Exception('Spark parameter is None !!')
        except Exception as error:
            raise Exception(error)
