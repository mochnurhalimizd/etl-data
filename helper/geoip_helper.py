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


class geoip:
    """
    Geo Ip class for convert ip to location.
    """
    
    @staticmethod
    def getConnection():
        return geoip2.database.Reader('/usr/share/geoip2/GeoIP2-City.mmdb')

    @staticmethod
    def get_locality_from_ip(ipaddress):
        """
        Get locality from ip address.

        Usage
        self.get_locality_from_ip('string ipaddress')

        :param ipadress (string) ip address from event.
        """
        db = geoip2.database.Reader('/usr/share/geoip2/GeoIP2-City.mmdb')
        
        response = None
        try:
            response = db.city(ipaddress)
            return ', '.join(
                [
                    response.country.name if response.country is not None else '',
                    geoip.get_subdivision(response),
                    response.city.name if response.city is not None else ''
                ]
            )
        except Exception:
            pass

        return ''

    @staticmethod
    def get_subdivision(response):
        if not response.subdivisions.most_specific.names:
            return ''
        else:
            return response.subdivisions.most_specific.names.get('en')


if __name__ == "__main__":
    geoip = geoip()
    print(geoip.get_locality_from_ip('114.125.79.98'))
