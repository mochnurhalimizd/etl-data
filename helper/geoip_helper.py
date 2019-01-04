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
import geoip2.database


class geoip(object):
    """
    Geo Ip class for convert ip to location.
    """

    def __init__(self, ipaddress):
        """Constructor
        
        :param ipadress (string) ip address from event.
        """

        self.ipaddress = ipaddress
        self.db = geoip2.database.Reader('/usr/share/geoip2/GeoIP2-City.mmdb')

    def get_locality_from_ip(self):
        """
        Get locality from ip address.

        Usage
        self.get_locality_from_ip('string ipaddress')

        :param ipadress (string) ip address from event.
        """
        response = self.db.city(self.ipaddress)
        return ', '.join([
            response.country.name if response.country is not None else '',
            response.subdivisions.most_specific.names['de'] if response.subdivisions is not None else '',
            response.city.name if response.city is not None else ''
        ])