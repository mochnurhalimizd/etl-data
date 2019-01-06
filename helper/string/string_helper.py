"""
Class String Helper

@author Moch Nurhalimi Zaini D <moch.nurhalimi@gmail.com>
@author Irfan Andriansyah <irfan@99.co>
"""

import base64


class StringHelper:
    """
    Helper for transform string
    """

    @staticmethod
    def decode_base64(text):
        """Decode base64, padding being optional.

        Usage
        self.decode_base64('string base64')

        :param text: (String) Base64 data as an ASCII byte string
        """

        missing_padding = 4 - len(text) % 4
        text += '=' * missing_padding if missing_padding else ''

        return base64.b64decode(text)

    @staticmethod
    def url_to_dict(url):
        """Parsing url into dictionary

        Usage
        self.decode_base64('string base64')

        :param url: (String) Url parameter
        """

        text = url[3:]

        return dict([StringHelper.test(item, '=') for item in text.split('&')])

    @staticmethod
    def test(item, separate):
        """Text split based on separate parameter

        Usage
        self.test('a=1', '=')

        :param item: (String) String to split
        :param separate: (String) Separate
        """

        param = item.split(separate)

        if len(param) >= 2:
            return (param[0], [param[1]])

        return False
