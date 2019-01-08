#!/bin/usr/python
# -*- coding: utf-8 -*-
"""Module Transform from tracker dataframe to form search analysis.

`Python Styling Guide <https://www.python.org/dev/peps/pep-0008/>`
`Docstring Guide <https://docs.python.org/devguide/documenting.html>`


   :platform: Unix, Windows
.. moduleauthor:: Moch.Nurhalimi Zaini D <moch.nurhalimi@gmail.com>
"""

import os
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from helper.parser_helper import Parser
from helper.spark.spark_helper import SparkHelper
import config


class SearchTracker:
    """
    Transform data for search tracker
    """

    def __init__(self):
        self.connection = SparkHelper.createConnection()
        self.parser = Parser()

    @staticmethod
    def get_udf(key, parse):
        """
        Get User definition function for transform data search tracker
        """
        return udf(lambda params: parse(key, params), StringType())

    def get_parquet(self):
        """
        Get Tracker parquet
        """
        return SparkHelper.readParquet(
            self.connection, config.DATA_FRAME_TRACKER_SOURCE
        )

    def main(self):
        """
        Main Class for transform tracket dataset to search dataset
        """

        search_listing_type_udf = self.get_udf(
            'listing_type', self.parser.parse_event_search
        )

        search_property_type_udf = self.get_udf(
            'property_type', self.parser.parse_event_search
        )

        search_rent_type_udf = self.get_udf(
            'rent_type', self.parser.parse_event_search
        )

        search_price_evaluation_udf = self.get_udf(
            'price_evaluation', self.parser.parse_event_search
        )

        search_certification_type_udf = self.get_udf(
            'certification_type', self.parser.parse_event_search
        )

        df_tracker = self.get_parquet(
        ).filter("event_url LIKE '%cari%' OR event_url LIKE '%search%'")

        df_tracker.select(
            search_listing_type_udf('event_url'
                                    ).alias('event_search_param_listing_type'),
            search_property_type_udf('event_url').
            alias('event_search_param_property_type'),
            search_rent_type_udf('event_url'
                                 ).alias('event_search_param_rent_type'),
            search_price_evaluation_udf('event_url').
            alias('event_search_param_price_evaluation'),
            search_certification_type_udf('event_url').
            alias('event_search_param_certification_type')
        ).show()


if __name__ == "__main__":
    search_transform = SearchTracker()
    search_transform.main()
