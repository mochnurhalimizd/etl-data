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

import json
import os
import re
import sys
import urllib.parse
from urllib.parse import parse_qs

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from helper.string.string_helper import StringHelper


class Parser:
    """
    Main process for extract and parse data
    """

    def get_context_username(self, path):
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
  
    def parse_event_search_listing_type(self, param):
        """
        Parser listing type from url
        """
        param = str(param)
        params = re.findall(r"\/listingType_(\w+)", param)
        if not params:
            params = re.findall(
                r"(dijual|disewa|jualsewa|Dijual|Disewa|jual|sewa)", param)
        for listing_type in params:
            return self.clean_listing_type(listing_type)

    def clean_listing_type(self, listing_type):
        """
        Clean listing type
        """
        listing_type = listing_type.lower()
        return self.clean_standard_listing_type(
            listing_type.replace('di', "").replace("%20", " ")
        )

    @staticmethod
    def clean_standard_listing_type(listing_type):
        """
        Standardlization listing type.
        """
        if listing_type == "jual":
            return "Sale"
        if listing_type == "sewa":
            return "Rent"
        return 'Any'

    def parse_event_search_property_type(self, param):
        """
        Parser property type from url
        """
        param = str(param)
        params = re.findall(r"\/propertyType_([A-Za-z_\-\s\%20\,]+)", param)
        if not params:
            params = re.findall(
                r"vila|properti|rumah|apartemen|ruko|komersial|tanah|kost",
                param)
        for property_type in params:
            return self.clean_property_type(property_type)

    def clean_property_type(self, property_type):
        """
        Clean Property type
        """
        property_type = property_type.lower()
        return self.clean_standard_property_type(
            property_type.replace("-", " ").replace("%20", " ")
        )

    def clean_standard_property_type(self, property_type):
        """
        Standardlization listing type.
        """
        return self.get_property_type(property_type)
    
    @staticmethod
    def get_property_type(property_type):
        """
        Get property type for standarization.
        """
        types = {
            'rumah': 'House',
            'apartemen': 'Apartment',
            'ruko': 'Ruko',
            'vila': 'Villa',
            'komersial': 'Commercial',
            'tanah': 'Land',
            'kost': 'Kost'
        }

        return types.get(property_type, 'Property')


# if __name__ == "__main__":
#     parser = parser()
#     # print(parser.parse_event_search_listing_type('https://www.99.co/id/cari/Rumah-disewa-di-Indonesia/location_indonesia/venueId_1006/marketType_0'))
#     print(parser.clean_standard_property_type('rumahd'))

