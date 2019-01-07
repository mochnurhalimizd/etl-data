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

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from helper.parser_helper import parser
import config

# Instantiate module
parser = parser()

# Create or replace spark session
spark = SparkSession.builder.appName("Spark for ETL data 99.co ID").config(
    "spark.some.config.option", "some-value"
).getOrCreate()

# Extract tracker dataset
df_tracker = spark.read.parquet(config.DATA_FRAME_TRACKER_SOURCE)

# Fitler tracker contain url path cari or search.
# df_tracker = df_tracker.filter(
#     "event_url LIKE '%cari%' OR event_url LIKE '%search%'"
# )

df_tracker.show()

df_tracker.select('event_url').show(20, False)