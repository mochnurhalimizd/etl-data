#!/bin/usr/python
# -*- coding: utf-8 -*-
"""Geo IP Helper for convert ip to location
`Python Styling Guide <https://www.python.org/dev/peps/pep-0008/>`
`Docstring Guide <https://docs.python.org/devguide/documenting.html>`

This module is used for filter snowplow event to count property visit.

.. module:: helper
   :platform: Unix, Windows
.. moduleauthor:: Moch Nurhalimi Zaini D <moch.nurhalimi@gmail.com>
"""
from geoip import open_database


class geoip(object):
    """
    Geo Ip class for convert ip to location
    """

    def __init__(self):
        """Constructor
        """
        self.db = open_database('/usr/share/geoip2/GeoIP2-City.mmdb')

    def get_city_from_ip(self):
        """
        Get city from ip
        """
        return self.db.lookup('172.68.133.151')
