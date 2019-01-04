#!/bin/usr/python
# -*- coding: utf-8 -*-
"""Lambda Processor for Property Counter.

`Python Styling Guide <https://www.python.org/dev/peps/pep-0008/>`
`Docstring Guide <https://docs.python.org/devguide/documenting.html>`

This module is used for filter snowplow event to count property visit.

.. module:: property_counter
   :platform: Unix, Windows
.. moduleauthor:: Moch Nurhalimi Zaini D <moch.nurhalimi@gmail.com>
"""
import os
import sys
import json
import base64
import urllib.parse

from pyspark.sql import SparkSession
from urllib.parse import parse_qs

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from helper.geoip_helper import geoip
from helper.string.string_helper import StringHelper

# from pyspark.sql.functions import udf

# from pyspark.sql.types import StringType


class mainProcess():
    """
    Main process for extract and parse data
    """

    def __init__(self):
        """Constructor
        """

        self.spark = SparkSession.builder.appName(
            "Spark for ETL data 99.co ID"
        ).config("spark.some.config.option", "some-value").getOrCreate()
        self.get_dataset()
        self.get_transform_udf()
        print(self.category)
        print(self.action)
        print(self.sessionID)
        print(self.visitorID)
        print(self.userID)
        print(self.url)

    def get_dataset(self):
        """
        Get dataset from path
        """

        self.df_trackers = self.spark.read.json("test/resources/tracker.txt")
        self.df_users = self.spark.read.csv(
            "test/resources/user.csv", header=True
        )

    def get_transform_udf(self):
        """
        docstring here
        """
        # self.category = udf(
        #     lambda path: self.parse_event(path, 'se_ca'), StringType()
        # )

        self.category = self.parse_event(
            "/i?e=se&se_ca=Property&se_ac=Visit&se_la=608919551&tv=js-2.5.1&tna=cf&aid=Urbanindo%20Desktop%20Site&p=web&tz=Asia%2FJakarta&lang=en-US&cs=UTF-8&f_pdf=1&f_qt=0&f_realp=1&f_wma=1&f_dir=0&f_fla=1&f_java=0&f_gears=0&f_ag=0&res=1366x768&cd=24&cookie=1&eid=72418977-de08-4244-971e-e1e14b91bbf4&dtm=1468405258160&vp=1366x595&ds=1349x741&vid=1&sid=9a469ec8-c034-45ab-8de9-a56c084d4301&duid=adc1cb0251da614d&fp=2867200236&refr=http%3A%2F%2Fwww.urbanindo.com%2Fezy%3Ftype%3Dlisting%26param%3D608919551%26gclid%3DCNfPw8WZ8M0CFcoTaAodM80Cew&url=http%3A%2F%2Fwww.urbanindo.com%2Fezy%3Ftype%3Dlisting%26param%3D608919551&cx=eyJzY2hlbWEiOiJpZ2x1OmNvbS5zbm93cGxvd2FuYWx5dGljcy5zbm93cGxvdy9jb250ZXh0cy9qc29uc2NoZW1hLzEtMC0wIiwiZGF0YSI6W3sidXNlclR5cGUiOiJHdWVzdCIsInNJZCI6IjE3NDkzZmU2ODNiNGRkNGMyYjNiZjUzODcyNjkzYjJiIn1dfQ",
            'se_ca'
        )

        self.action = self.parse_event(
            "/i?e=se&se_ca=Property&se_ac=Visit&se_la=608919551&tv=js-2.5.1&tna=cf&aid=Urbanindo%20Desktop%20Site&p=web&tz=Asia%2FJakarta&lang=en-US&cs=UTF-8&f_pdf=1&f_qt=0&f_realp=1&f_wma=1&f_dir=0&f_fla=1&f_java=0&f_gears=0&f_ag=0&res=1366x768&cd=24&cookie=1&eid=72418977-de08-4244-971e-e1e14b91bbf4&dtm=1468405258160&vp=1366x595&ds=1349x741&vid=1&sid=9a469ec8-c034-45ab-8de9-a56c084d4301&duid=adc1cb0251da614d&fp=2867200236&refr=http%3A%2F%2Fwww.urbanindo.com%2Fezy%3Ftype%3Dlisting%26param%3D608919551%26gclid%3DCNfPw8WZ8M0CFcoTaAodM80Cew&url=http%3A%2F%2Fwww.urbanindo.com%2Fezy%3Ftype%3Dlisting%26param%3D608919551&cx=eyJzY2hlbWEiOiJpZ2x1OmNvbS5zbm93cGxvd2FuYWx5dGljcy5zbm93cGxvdy9jb250ZXh0cy9qc29uc2NoZW1hLzEtMC0wIiwiZGF0YSI6W3sidXNlclR5cGUiOiJHdWVzdCIsInNJZCI6IjE3NDkzZmU2ODNiNGRkNGMyYjNiZjUzODcyNjkzYjJiIn1dfQ",
            'se_ac'
        )

        self.label = self.parse_event(
            "/i?e=se&se_ca=Property&se_ac=Visit&se_la=608919551&tv=js-2.5.1&tna=cf&aid=Urbanindo%20Desktop%20Site&p=web&tz=Asia%2FJakarta&lang=en-US&cs=UTF-8&f_pdf=1&f_qt=0&f_realp=1&f_wma=1&f_dir=0&f_fla=1&f_java=0&f_gears=0&f_ag=0&res=1366x768&cd=24&cookie=1&eid=72418977-de08-4244-971e-e1e14b91bbf4&dtm=1468405258160&vp=1366x595&ds=1349x741&vid=1&sid=9a469ec8-c034-45ab-8de9-a56c084d4301&duid=adc1cb0251da614d&fp=2867200236&refr=http%3A%2F%2Fwww.urbanindo.com%2Fezy%3Ftype%3Dlisting%26param%3D608919551%26gclid%3DCNfPw8WZ8M0CFcoTaAodM80Cew&url=http%3A%2F%2Fwww.urbanindo.com%2Fezy%3Ftype%3Dlisting%26param%3D608919551&cx=eyJzY2hlbWEiOiJpZ2x1OmNvbS5zbm93cGxvd2FuYWx5dGljcy5zbm93cGxvdy9jb250ZXh0cy9qc29uc2NoZW1hLzEtMC0wIiwiZGF0YSI6W3sidXNlclR5cGUiOiJHdWVzdCIsInNJZCI6IjE3NDkzZmU2ODNiNGRkNGMyYjNiZjUzODcyNjkzYjJiIn1dfQ",
            'se_la'
        )

        self.sessionID = self.label = self.parse_event(
            "/i?e=se&se_ca=Property&se_ac=Visit&se_la=608919551&tv=js-2.5.1&tna=cf&aid=Urbanindo%20Desktop%20Site&p=web&tz=Asia%2FJakarta&lang=en-US&cs=UTF-8&f_pdf=1&f_qt=0&f_realp=1&f_wma=1&f_dir=0&f_fla=1&f_java=0&f_gears=0&f_ag=0&res=1366x768&cd=24&cookie=1&eid=72418977-de08-4244-971e-e1e14b91bbf4&dtm=1468405258160&vp=1366x595&ds=1349x741&vid=1&sid=9a469ec8-c034-45ab-8de9-a56c084d4301&duid=adc1cb0251da614d&fp=2867200236&refr=http%3A%2F%2Fwww.urbanindo.com%2Fezy%3Ftype%3Dlisting%26param%3D608919551%26gclid%3DCNfPw8WZ8M0CFcoTaAodM80Cew&url=http%3A%2F%2Fwww.urbanindo.com%2Fezy%3Ftype%3Dlisting%26param%3D608919551&cx=eyJzY2hlbWEiOiJpZ2x1OmNvbS5zbm93cGxvd2FuYWx5dGljcy5zbm93cGxvdy9jb250ZXh0cy9qc29uc2NoZW1hLzEtMC0wIiwiZGF0YSI6W3sidXNlclR5cGUiOiJHdWVzdCIsInNJZCI6IjE3NDkzZmU2ODNiNGRkNGMyYjNiZjUzODcyNjkzYjJiIn1dfQ",
            'sid'
        )

        self.visitorID = self.parse_event(
            "/i?e=se&se_ca=Property&se_ac=Visit&se_la=608919551&tv=js-2.5.1&tna=cf&aid=Urbanindo%20Desktop%20Site&p=web&tz=Asia%2FJakarta&lang=en-US&cs=UTF-8&f_pdf=1&f_qt=0&f_realp=1&f_wma=1&f_dir=0&f_fla=1&f_java=0&f_gears=0&f_ag=0&res=1366x768&cd=24&cookie=1&eid=72418977-de08-4244-971e-e1e14b91bbf4&dtm=1468405258160&vp=1366x595&ds=1349x741&vid=1&sid=9a469ec8-c034-45ab-8de9-a56c084d4301&duid=adc1cb0251da614d&fp=2867200236&refr=http%3A%2F%2Fwww.urbanindo.com%2Fezy%3Ftype%3Dlisting%26param%3D608919551%26gclid%3DCNfPw8WZ8M0CFcoTaAodM80Cew&url=http%3A%2F%2Fwww.urbanindo.com%2Fezy%3Ftype%3Dlisting%26param%3D608919551&cx=eyJzY2hlbWEiOiJpZ2x1OmNvbS5zbm93cGxvd2FuYWx5dGljcy5zbm93cGxvdy9jb250ZXh0cy9qc29uc2NoZW1hLzEtMC0wIiwiZGF0YSI6W3sidXNlclR5cGUiOiJHdWVzdCIsInNJZCI6IjE3NDkzZmU2ODNiNGRkNGMyYjNiZjUzODcyNjkzYjJiIn1dfQ",
            'fp'
        )

        self.userID = self.parse_user_ID(
            "/i?e=se&se_ca=Property&se_ac=Visit&se_la=608919551&tv=js-2.5.1&tna=cf&aid=Urbanindo%20Desktop%20Site&p=web&tz=Asia%2FJakarta&lang=en-US&cs=UTF-8&f_pdf=1&f_qt=0&f_realp=1&f_wma=1&f_dir=0&f_fla=1&f_java=0&f_gears=0&f_ag=0&res=1366x768&cd=24&cookie=1&eid=72418977-de08-4244-971e-e1e14b91bbf4&dtm=1468405258160&vp=1366x595&ds=1349x741&vid=1&sid=9a469ec8-c034-45ab-8de9-a56c084d4301&duid=adc1cb0251da614d&fp=2867200236&refr=http%3A%2F%2Fwww.urbanindo.com%2Fezy%3Ftype%3Dlisting%26param%3D608919551%26gclid%3DCNfPw8WZ8M0CFcoTaAodM80Cew&url=http%3A%2F%2Fwww.urbanindo.com%2Fezy%3Ftype%3Dlisting%26param%3D608919551&cx=eyJzY2hlbWEiOiJpZ2x1OmNvbS5zbm93cGxvd2FuYWx5dGljcy5zbm93cGxvdy9jb250ZXh0cy9qc29uc2NoZW1hLzEtMC0wIiwiZGF0YSI6W3sidXNlclR5cGUiOiJHdWVzdCIsInVzZXJJZCI6InJheWF2ZXJvbmljYSIsImxvY2F0aW9uIjoiQmFuZHVuZyJ9XX0=",
            "uid"
        )

        self.url = self.parse_event(
            "/i?e=se&se_ca=Property&se_ac=Visit&se_la=608919551&tv=js-2.5.1&tna=cf&aid=Urbanindo%20Desktop%20Site&p=web&tz=Asia%2FJakarta&lang=en-US&cs=UTF-8&f_pdf=1&f_qt=0&f_realp=1&f_wma=1&f_dir=0&f_fla=1&f_java=0&f_gears=0&f_ag=0&res=1366x768&cd=24&cookie=1&eid=72418977-de08-4244-971e-e1e14b91bbf4&dtm=1468405258160&vp=1366x595&ds=1349x741&vid=1&sid=9a469ec8-c034-45ab-8de9-a56c084d4301&duid=adc1cb0251da614d&fp=2867200236&refr=http%3A%2F%2Fwww.urbanindo.com%2Fezy%3Ftype%3Dlisting%26param%3D608919551%26gclid%3DCNfPw8WZ8M0CFcoTaAodM80Cew&url=http%3A%2F%2Fwww.urbanindo.com%2Fezy%3Ftype%3Dlisting%26param%3D608919551&cx=eyJzY2hlbWEiOiJpZ2x1OmNvbS5zbm93cGxvd2FuYWx5dGljcy5zbm93cGxvdy9jb250ZXh0cy9qc29uc2NoZW1hLzEtMC0wIiwiZGF0YSI6W3sidXNlclR5cGUiOiJHdWVzdCIsInNJZCI6IjE3NDkzZmU2ODNiNGRkNGMyYjNiZjUzODcyNjkzYjJiIn1dfQ",
            'url'
        )

    def parse_data_set(self):
        """
        Parsing data from dataset to get key value of event.
        """
        # self.df.select('path', self.category('path').alias('category'))

    def parse_user_ID(self, path, event):
        """
        Context string is encoded in base64, but somecase it separated with url encoded characters.
        """

        return self.parse_event(path, event) if self.parse_event(
            path, event
        ) is not None else self.get_userid_from_context_array(path)

    def get_userid_from_context_array(self, path):
        """
        Get userID from context array that encoded with base64
        """

        userID = self.df_users.filter(
            self.df_users.username == self.get_context_userid(path)
        ).select('id').collect()

        if len(userID) >= 1:
            return userID[0][0]
        return None

    def get_context_userid(self, path):
        """
        Get context userid array that encoded with base64
        """

        for cx_data in self.get_context_array(path).get('data'):
            if cx_data.get('userId') is not None:
                return cx_data.get('userId')
        return None

    def get_context_array(self, path):
        """
        Context string is encoded in base64, but somecase it separated with url encoded characters.
        """

        context_string = urllib.parse.unquote(
            urllib.parse.unquote(self.parse_event(path, 'cx'))
        )
        context_string = context_string.replace('\n', ' ').replace('\r', '')

        return json.loads(StringHelper.decode_base64(context_string))

    def parse_event(self, path, event):
        """
        docstring here
        """
        if parse_qs(path).get(event) is not None:
            return parse_qs(path).get(event)[0]

        return None


if __name__ == "__main__":
    mainProcess()
