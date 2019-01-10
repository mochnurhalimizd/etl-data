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

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from helper.parser_helper import Parser  # noqa
from helper.spark.spark_helper import SparkHelper  # noqa
from helper.config.config_helper import get_config  # noqa


class SearchTracker:
    """
    Transform data for search tracker
    """

    def __init__(self):
        self.connection = SparkHelper.create_connection()
        self.parser = Parser()
        self.config = get_config()

    def get_parquet(self):
        """
        Get Tracker parquet
        """
        return SparkHelper.read_parquet(
            self.connection, self.config.get('data_source', 'result')
        )

    def main(self):
        """
        Main Class for transform tracket dataset to search dataset
        """

        search_listing_type_udf = SparkHelper.get_udf(
            'listing_type', self.parser.parse_event_search
        )

        search_property_type_udf = SparkHelper.get_udf(
            'property_type', self.parser.parse_event_search
        )

        search_rent_type_udf = SparkHelper.get_udf(
            'rent_type', self.parser.parse_event_search
        )

        search_price_evaluation_udf = SparkHelper.get_udf(
            'price_evaluation', self.parser.parse_event_search
        )

        search_certification_type_udf = SparkHelper.get_udf(
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
    SearchTracker().main()
