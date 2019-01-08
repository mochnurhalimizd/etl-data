import os
import sys

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from helper.parser_helper import Parser  # noqa
from helper.spark.spark_helper import SparkHelper  # noqa
from helper.config.config_helper import get_config  # noqa


class PropertyTracker:
    def __init__(self):
        self.connection = SparkHelper.create_connection()
        self.parser = Parser()
        self.config = get_config()

    def get_parquet(self):
        return SparkHelper.read_parquet(
            self.connection, self.config.get('data_source', 'result')
        )

    def main(self):
        self.get_parquet().show()


if __name__ == '__main__':
    PropertyTracker().main()
