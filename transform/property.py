"""
Class Property Transform

@author Irfan Andriansyah <irfan@99.co>
"""

import os
import sys
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from helper.parser_helper import Parser  # noqa
from helper.spark.spark_helper import SparkHelper  # noqa
from helper.config.config_helper import get_config  # noqa


class PropertyTracker:
    """
    Transform data for property tracker
    """

    def __init__(self):
        self.connection = SparkHelper.create_connection()
        self.parser = Parser()
        self.config = get_config()

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
        return SparkHelper.read_parquet(
            self.connection, self.config.get('data_source', 'result')
        )

    def main(self):
        """
        Main Class for transform tracket dataset to search dataset
        """

        df_tracker = self.get_parquet().filter("event_category = 'Property'")
        df_property = self.connection.read.csv(
            "test/resources/properties.csv", header=True
        )

        retval = df_tracker.join(
            df_property, df_tracker.event_label == df_property.id
        )

        retval.select(
            df_tracker.time.alias('event_created_at'),
            df_property.id.alias('properties_id'),
            df_property.title.alias('properties_title'),
            df_property.listingType.alias('listing_type'),
            df_property.price.alias('property_price'),
            df_property.localityString.alias('property_locality'),
            df_property.latitude.alias('property_latitude'),
            df_property.longitude.alias('property_longitude'),
            df_property.showOnLanding.alias('property_is_show_on_landing'),
            df_property.featureType.alias('property_is_featured_type'),
            df_property.localityId.alias('property_locality_id'),
            df_property.marketType.alias('property_market_type'),
            df_tracker.event_sessionID.alias('session_id')
        ).show()


if __name__ == '__main__':
    PropertyTracker().main()
