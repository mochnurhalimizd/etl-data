#!/bin/usr/python
# -*- coding: utf-8 -*-
"""Module Transform from tracker data frame to form search analysis.

`Python Styling Guide <https://www.python.org/dev/peps/pep-0008/>`
`Docstring Guide <https://docs.python.org/devguide/documenting.html>`


   :platform: Unix, Windows
.. moduleauthor:: Made <made@99.co>
"""

import os
import re
import sys

from pyspark.sql.functions import udf
from pyspark.sql.types import StringType

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from helper.parser_helper import Parser  # noqa
from helper.spark.spark_helper import SparkHelper  # noqa
from helper.config.config_helper import get_config  # noqa

_CATEGORY = 'event_category'
_REFERRAL = 'event_refr'
_NEW_REFERRAL = 'event_referral'


class AgentTracker:
    """
    Transform data for agent tracker
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

    @staticmethod
    def create_column_event_referral(response):
        """
        Mapping referral by url, step by step
        1. remove 'http' or 'https' and 'www' with regex_url
        2. search only url 99 or urbanindo with regex_99
        3. remove ('co' or 'com' or 'co.id') and '/blablabla' with regex_com
        :param response Response.
        :return url
        """
        regex_url, regex_99, regex_com = r'(http(s)?://)(www.)?', r'^(99|urbanindo)', r'(\.co(m)?(\.id)?(.*))'
        url = re.sub(regex_url, '', str(response))
        if re.search(regex_99, url):
            url = get_referral_by_url_99(url)
        else:
            url = re.sub(regex_com, '', url)
        return url

    def main(self):
        tracker_df = self.get_parquet()
        referral_udf = udf(self.create_column_event_referral, StringType())

        # Filter event category is like Agent
        agent_df = tracker_df\
            .filter(tracker_df[_CATEGORY].rlike('Agent'))\
            .filter(tracker_df[_CATEGORY] != '')\
            .withColumn(_NEW_REFERRAL, referral_udf(tracker_df[_REFERRAL]))

        agent_df.show(100)


def get_referral_by_url_99(url):
    """
    Get referral only by schema url 99 or urbanindo
    :param url Url.
    :return url
    """
    arrays = url.split('/')
    url = arrays[0]
    if len(arrays) > 2 and arrays[2] in ['cari', 'properti', 'account']:
        url += '-{}'.format(arrays[2])
        if arrays[2] == 'properti' and len(arrays) > 3:
            keywords = arrays[3].split('-')
            url += '-{}'.format(keywords[len(keywords) - 1])
    return url


if __name__ == '__main__':
    AgentTracker().main()
