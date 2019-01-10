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
from helper.config.config_helper import translate_type


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

    def parse_event_search(self, event_search, params):
        """
        Parser event search url.
        """
        return {
            'listing_type': self.parse_event_search_listing_type,
            'property_type': self.parse_event_search_property_type,
            'rent_type': lambda param, event_search: self.regex_parser(
                r"\/rentType_(\w+)",
                param,
                event_search
            ),
            'price_evaluation': lambda param, event_search: self.regex_parser(
                r"\/hasPriceEvaluation_([0-9\,]+)",
                param,
                event_search
            ),
            'certification_type':
            lambda param, event_search: self.regex_parser(
                r"\/certificationType_([0-9\,]+)",
                param,
                event_search
            ),
            'min_price': self.parse_min_price,
            'max_price': self.parse_max_price,
            'keyword': lambda param, event_search: self.regex_parser(
                r"\/keywords_([A-Za-z_\-\s\%20\,]+)",
                param,
                event_search=None),
            'land_size': lambda param, event_search: self.regex_parser(
                r"\/landSize_([0-9\,]+)",
                param,
                event_search=None),
            'min_land_size': self.parse_min_land_size,
            'max_land_size': self.parse_max_land_size,
            'min_building_size': self.parse_min_building_size,
            'max_building_size': self.parse_max_building_size,
            'min_numbers_bedroom': self.parse_min_number_bedrooms,
            'max_numbers_bedroom': self.parse_max_number_bedrooms,
            'min_numbers_bathroom': self.parse_min_number_bathrooms,
            'max_numbers_bathroom': self.parse_max_number_bathrooms,
            'sort_type': self.parse_sort,
            'pagination': self.parse_pagination
        }[event_search](params, event_search)

    def regex_parser(self, regex_exp, param, event_search=None):
        """
        Regex parser for get param event search
        """
        params = re.findall(regex_exp, param)
        for value in params:
            if event_search is not None:
                return self.clean_standard_type(
                    '_'.join([event_search, value])
                )
            return self.clean_replace(value)
        return None
    
    @staticmethod
    def clean_replace(value):
        """
        Regex parser for get param event search
        """
        for rule_replace in (
                ('juta', '000000'),
                ('jt', '000000'),
                ('mily', '000000000'),
                ('mil', '000000000'),
                ("%20", " ")
                ):
            value = value.replace(*rule_replace)
        return value

    def parse_event_search_listing_type(self, param, _):
        """
        Parser listing type from url
        """
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
        return self.clean_standard_type(
            listing_type.replace('di', "").replace("%20", " ")
        )

    def parse_event_search_property_type(self, param, _):
        """
        Parser property type from url
        """
        params = re.findall(r"\/propertyType_([A-Za-z_\-\s\%20\,]+)", param)
        if not params:
            params = re.findall(
                r"vila|properti|rumah|apartemen|ruko|komersial|tanah|kost|ruang-kantor|gudang|hotel|pabrik|kios|kiosk|factory|gedung-bertingkat|kondotel|condotel|toko|store",
                param)
        for property_type in params:
            return self.clean_property_type(property_type)

    def clean_property_type(self, property_type):
        """
        Clean Property type
        """
        property_type = property_type.lower()
        return self.clean_standard_type(
            property_type.replace("-", " ").replace("%20", " ")
        )

    def clean_standard_type(self, event_type=None):
        """
        Standardrization listing type.
        """
        return translate_type(event_type)

    def parse_pagination(self, params, _):
        """
        Parse pagination in search param URL.
        """
        sort_type = self.regex_parser(
            r"page_([0-9\,]+)",
            params,
            event_search=None)
        return sort_type if sort_type is not None else None
    
    def parse_sort(self, params, _):
        """
        Parse sorting type in search param URL.
        """
        sort_type = self.regex_parser(
            r"sort_([0-9\,]+)",
            params,
            event_search=None)
        return sort_type if sort_type is not None else self.regex_parser(
            r"Urut_([0-9\,]+)",
            params,
            event_search=None)

    def parse_max_number_bathrooms(self, params, _):
        """
        Parse Min Number of bathroom in search param URL.
        """
        bathroom = self.get_params(params, r"\/bathrooms_([0-9\,]+)")
        _, max_bathroom = bathroom
        if max_bathroom is not None:
            return max_bathroom
        max_bathroom = self.regex_parser(
            r"MaksKamarMandi_([0-9\,]+)",
            params,
            None)
        return max_bathroom if max_bathroom is not None else None

    def parse_min_number_bathrooms(self, params, _):
        """
        Parse Min Number of bathroom in search param URL.
        """
        bathroom = self.get_params(params, r"\/bathrooms_([0-9\,]+)")
        min_bathroom, _ = bathroom
        if min_bathroom is not None:
            return min_bathroom
        min_bathroom = self.regex_parser(
            r"MinKamarMandi_([0-9\,]+)",
            params,
            None)
        return min_bathroom if min_bathroom is not None else None

    def parse_min_number_bedrooms(self, params, _):
        """
        Parse Number of bedrooms in search param URL.
        """
        bedrooms = self.get_params(params, r"\/bedrooms_([0-9\,]+)")
        min_bedroom, _ = bedrooms
        if min_bedroom is not None:
            return min_bedroom
        min_bedroom = self.regex_parser(
            r"MinKamarTidur_([0-9\,]+)",
            params,
            None)
        return min_bedroom if min_bedroom is not None else None
    
    def parse_max_number_bedrooms(self, params, _):
        """
        Parse Max Number of bedrooms in search param URL.
        """
        bedrooms = self.get_params(params, r"\/bedrooms_([0-9\,]+)")
        _, max_bedroom = bedrooms
        if max_bedroom is not None:
            return max_bedroom
        max_bedroom = self.regex_parser(
            r"MaksKamarTidur_([0-9\,]+)",
            params,
            None)
        return max_bedroom if max_bedroom is not None else None

    def parse_max_building_size(self, params, _):
        """
        Parse Min Building size in search param URL.
        """
        building_size = self.get_params(params, r"\/buildingSize_([0-9\,]+)")
        _, max_biilding_size = building_size
        if max_biilding_size is not None:
            return max_biilding_size
        max_biilding_size = self.regex_parser(
            r"\/MaksLuasBangunan_([0-9\,]+)",
            params,
            None)
        return max_biilding_size if max_biilding_size is not None else None

    def parse_min_building_size(self, params, _):
        """
        Parse Min Building size in search param URL.
        """
        building_size = self.get_params(params, r"\/buildingSize_([0-9\,]+)")
        min_biilding_size, _ = building_size
        if min_biilding_size is not None:
            return min_biilding_size
        min_biilding_size = self.regex_parser(
            r"\/MinLuasBangunan_([0-9\,]+)",
            params,
            None)
        return min_biilding_size if min_biilding_size is not None else None
    
    def parse_max_land_size(self, params, _):
        """
        Parse Maximum Land size in search param URL.
        """
        land_size = self.get_params(params, r"\/landSize_([0-9\,]+)")
        _, max_land_size = land_size
        if max_land_size is not None:
            return max_land_size
        max_land_size = self.regex_parser(
            r"\/MaksLuasTanah_([0-9\,]+)",
            params,
            None)
        return max_land_size if max_land_size is not None else None
    
    def parse_min_land_size(self, params, _):
        """
        Parse Min Land size in search param URL.
        """
        land_size = self.get_params(params, r"\/landSize_([0-9\,]+)")
        min_land_size, _ = land_size
        if min_land_size is not None:
            return min_land_size
        min_land_size = self.regex_parser(
            r"\/MinLuasTanah_([0-9\,]+)",
            params,
            None)
        return min_land_size if min_land_size is not None else None

    def parse_min_price(self, params, _):
        """
        Parse Minimal price in search param URL.
        """
        price = self.get_params(params, r"\/harga_([0-9\,]+)")
        min_price, _ = price
        if min_price is not None:
            return min_price
        min_price = self.regex_parser(
            r"\/MinHarga_([0-9]+)",
            params,
            None)
        return min_price if min_price is not None else self.regex_parser(
            r"min-([0-9,A-Za-z]+)",
            params,
            None)
        
    def parse_max_price(self, params, _):
        """
        Parse Minimal price in search param URL.
        """
        price = self.get_params(params, r"\/harga_([0-9\,]+)")
        _, max_price = price
        if max_price is not None:
            return max_price
        max_price = self.regex_parser(
            r"\/MaksHarga_([0-9]+)",
            params,
            None)
        return max_price if max_price is not None else self.regex_parser(
            r"maks-([0-9,A-Za-z]+)",
            params,
            None)

    @staticmethod
    def get_params(param, regex):
        """
        Parse price in param.
        """
        min_value, max_value = None, None
        params = re.findall(regex, param)
        for param in params:
            value = param.split(",")
            if len(value) > 1:
                min_value, max_value = value
                if not max_value:
                    max_value = None
            else:
                min_value = value[0]
        return min_value, max_value


if __name__ == "__main__":
    Parser = Parser()
    # event_param = Parser.parse_event_search('max_price', 'https://www.99.co/id/cari/Rumah-dijual-di-Probolinggo%2C-Jawa-Timur-min-100jt-maks-5mily/location_probolinggo,%20jawa%20timur/listingType_sale/propertyType_house/radius_-1/harga_100000000,5000000000/certificationType_0/marketType_0')
    # event_param = Parser.parse_event_search('min_land_size', 'https://www.99.co/id/cari/tanah/dijual/yos-sudarso/MinLuasTanah_1500/Hlmn_3')
    event_param = Parser.parse_event_search('sort_type', 'https://www.99.co/id/cari/Rumah-dijual-di-BTN-LINGKAR-PRATAMA-Mataram-Nusa-Tenggara-Barat/location_nusa%20tenggara%20barat,%20mataram/coordinate_-8.5769951,116.1004894:1000/radius_1000/longitude_116.1004894/latitude_-8.5769951/sort_9/page_3#main-search-container')
    print(event_param)
