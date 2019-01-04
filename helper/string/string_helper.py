"""
Class String Helper

@author Moch Nurhalimi Zaini D <moch.nurhalimi@gmail.com>
@author Irfan Andriansyah <irfan@99.co>
"""

import base64


class StringHelper(object):
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
