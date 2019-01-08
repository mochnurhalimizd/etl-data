"""
Spark Helper Test Case

@author Irfan Andriansyah <irfan@99.co>
"""
import pytest
from pyspark.sql.session import SparkSession
from pyspark.sql.dataframe import DataFrame
from helper.spark.spark_helper import SparkHelper as S


def test_create_connection():
    """
    test case for method create_connection in SpackHelper
    """

    assert isinstance(S.create_connection(), SparkSession)
    assert S.create_connection(
    ).conf.get('spark.some.config.option') == 'some-value'


def test_read_parquet():
    """
    test case for method read_parquet in SpackHelper
    """

    connection = S.create_connection()
    with pytest.raises(Exception):
        S.read_parquet(connection, 'folder_is_not_exists')

    assert isinstance(
        S.read_parquet(connection, 'test/resources/result'), DataFrame
    )
    assert 'event_label' in S.read_parquet(
        connection, 'test/resources/result'
    ).columns
