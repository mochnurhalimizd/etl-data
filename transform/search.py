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

        search_min_price_udf = SparkHelper.get_udf(
            'min_price', self.parser.parse_event_search
        )

        search_max_price_udf = SparkHelper.get_udf(
            'max_price', self.parser.parse_event_search
        )

        search_keyword_udf = SparkHelper.get_udf(
            'keyword', self.parser.parse_event_search
        )

        search_min_land_size_udf = SparkHelper.get_udf(
            'min_land_size', self.parser.parse_event_search
        )

        search_max_land_size_udf = SparkHelper.get_udf(
            'max_land_size', self.parser.parse_event_search
        )

        search_min_building_size_udf = SparkHelper.get_udf(
            'min_building_size', self.parser.parse_event_search
        )

        search_max_building_size_udf = SparkHelper.get_udf(
            'max_building_size', self.parser.parse_event_search
        )

        search_min_bedroom_size_udf = SparkHelper.get_udf(
            'min_numbers_bedroom', self.parser.parse_event_search
        )

        search_max_bedroom_size_udf = SparkHelper.get_udf(
            'max_numbers_bedroom', self.parser.parse_event_search
        )

        search_min_bathroom_size_udf = SparkHelper.get_udf(
            'min_numbers_bathroom', self.parser.parse_event_search
        )

        search_max_bathroom_size_udf = SparkHelper.get_udf(
            'max_numbers_bathroom', self.parser.parse_event_search
        )

        search_sort_type_size_udf = SparkHelper.get_udf(
            'sort_type', self.parser.parse_event_search
        )

        search_pagination_udf = SparkHelper.get_udf(
            'pagination', self.parser.parse_event_search
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
            alias('event_search_param_certification_type'),
            search_min_price_udf('event_url'
                                 ).alias('event_search_param_min_price'),
            search_max_price_udf('event_url'
                                 ).alias('event_search_param_max_price'),
            search_keyword_udf('event_url'
                               ).alias('event_search_param_keyword'),
            search_min_land_size_udf('event_url').
            alias('event_search_param_min_land_size'),
            search_max_land_size_udf('event_url').
            alias('event_search_param_max_land_size'),
            search_min_building_size_udf('event_url').
            alias('event_search_param_min_building_size'),
            search_max_building_size_udf('event_url').
            alias('event_search_param_max_building_size'),
            search_min_bedroom_size_udf('event_url').
            alias('event_search_param_min_bedrooms_building_size'),
            search_max_bedroom_size_udf('event_url').
            alias('event_search_param_max_bedrooms_building_size'),
            search_min_bathroom_size_udf('event_url').
            alias('event_search_param_min_bathrooms_building_size'),
            search_max_bathroom_size_udf('event_url').
            alias('event_search_param_max_bathrooms_building_size'),
            search_sort_type_size_udf('event_url').
            alias('event_search_param_sort_type'),
            search_pagination_udf('event_url').
            alias('event_search_param_pagination')
        ).show()



if __name__ == "__main__":
    SearchTracker().main()
