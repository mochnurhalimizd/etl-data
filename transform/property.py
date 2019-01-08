import os
import sys

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from helper.parser_helper import Parser
from helper.spark.spark_helper import SparkHelper
import config


class PropertyTracker:
    def __init__(self):
        self.connection = SparkHelper.createConnection()
        self.parser = Parser()

    def getParquet(self):
        return SparkHelper.readParquet(
            self.connection, config.DATA_FRAME_TRACKER_SOURCE
        )

    def main(self):
        self.getParquet().show()


if __name__ == '__main__':
    a = PropertyTracker()
    a.main()