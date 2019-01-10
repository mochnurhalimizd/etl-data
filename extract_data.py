#!/bin/usr/python
# -*- coding: utf-8 -*-
"""Module Extract from tracker raw data and parser, load to tracker dataframe (Py).

`Python Styling Guide <https://www.python.org/dev/peps/pep-0008/>`
`Docstring Guide <https://docs.python.org/devguide/documenting.html>`


   :platform: Unix, Windows
.. moduleauthor:: Moch.Nurhalimi Zaini D <moch.nurhalimi@gmail.com>
"""

import os
import sys
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from helper.parser_helper import Parser  # noqa
from helper.spark.spark_helper import SparkHelper  # noqa
from helper.config.config_helper import get_config  # noqa
from helper.geoip_helper import geoip  # noqa


class ExtractData:
    """
    Extract data tracker from bucket snowplow.log.urbanindo.com
    """

    def __init__(self):
        self.connection = SparkHelper.create_connection()
        self.parser = Parser()
        self.config = get_config()
        self.result = self.config.get('data_source', 'result')
        self.geoip = geoip()
        self.db_geoip = self.geoip.getConnection()
        # self.db_geoip = self.geoip.db_geoip

    def get_snowplow_data(self):
        """
        Get data from bucket s3 snowplow.log.urbanindo.com
        """
        return SparkHelper.read_json(
            self.connection, self.config.get('data_source', 'tracker')
        )

    def main(self):
        """
        Main method for transform raw snowplow data to dataset tracker
        """

        event_label_udf = SparkHelper.get_udf('se_la', self.parser.parse_event)

        event_aid_udf = SparkHelper.get_udf('aid', self.parser.parse_event)

        event_url_udf = SparkHelper.get_udf('url', self.parser.parse_event)

        event_refr_udf = SparkHelper.get_udf('refr', self.parser.parse_event)

        event_tz_udf = SparkHelper.get_udf('tz', self.parser.parse_event)

        event_category_udf = SparkHelper.get_udf(
            'se_ca', self.parser.parse_event
        )

        event_action_udf = SparkHelper.get_udf(
            'se_ac', self.parser.parse_event
        )

        event_sessionid_udf = SparkHelper.get_udf(
            'sid', self.parser.parse_event
        )

        event_visitorid_udf = SparkHelper.get_udf(
            'fp', self.parser.parse_event
        )

        event_platform_type_udf = SparkHelper.get_udf(
            'p', self.parser.parse_event
        )

        # db = self.db_geoip

        event_ip_to_locality = udf(
            self.geoip.get_locality_from_ip, StringType()
        )

        df_tracker = self.get_snowplow_data()
        df_tracker.select(
            event_ip_to_locality(df_tracker.ip).alias('event_ip_to_locality'),
            df_tracker.ip.alias('event_ip'),
            df_tracker.time.alias('event_time'),
            # event_category_udf('path').alias('event_category'),
            # event_tz_udf('path').alias('event_tz'),
            # event_action_udf('path').alias('event_action'),
            # event_label_udf('path').alias('event_label'),
            # event_sessionid_udf('path').alias('event_sessionID'),
            # event_visitorid_udf('path').alias('event_visitorID'),
            # event_platform_type_udf('path').alias('event_platform_type'),
            # event_aid_udf('path').alias('event_aid'),
            # event_url_udf('path').alias('event_url'),
            # event_refr_udf('path').alias('event_refr')
        ).show(20, False)
        # .write.parquet(self.result)


if __name__ == "__main__":
    Extract_data = ExtractData()
    Extract_data.main()
