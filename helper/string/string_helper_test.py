"""
String Helper Test Case

@author Irfan Andriansyah <irfan@99.co>
"""

import json
from helper.string.string_helper import StringHelper


def test_decode_base64():
    """
    test case for method decode_base64 in String Helper
    """

    sample_string = """
    eyJzY2hlbWEiOiJpZ2x1OmNvbS5zbm93cGx
    vd2FuYWx5dGljcy5zbm93cGxvdy9jb250ZXh0cy9qc29uc2NoZW1hL
    zEtMC0wIiwiZGF0YSI6W3sidXNlclR5cGUiOiJHdWVzdCIsInVzZXJ
    JZCI6InJheWF2ZXJvbmljYSIsImxvY2F0aW9uIjoiQmFuZHVuZyJ9XX0=
    """

    response = json.loads(StringHelper.decode_base64(sample_string))

    assert response.get('data')[0]['userId'] == 'rayaveronica'
