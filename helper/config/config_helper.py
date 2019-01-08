"""
Class Spark Helper

@author Irfan Andriansyah <irfan@99.co>

Documentation:
https://docs.python.org/3/library/configparser.html
"""

import os
import json
from configparser import ConfigParser


def get_config(files='config.conf'):
    """Generate object config using ConfigParser.

    Usage
    get_config('config-name.conf')

    :param files: (String) config file name.
    """

    try:
        config = ConfigParser()
        config.read(
            os.path.join(
                os.path.dirname(
                    os.path.dirname(
                        os.path.dirname(os.path.abspath(__file__))
                    )
                ), files
            )
        )

        return config
    except Exception as error:
        raise Exception(error)


def translate_type(event_type):
    """Get property from config.

    Usage
    translate_type('house / apartment / etc')

    :param files: (String) key dictionary.
    """

    conf = get_config()

    return json.loads(conf.get('key', 'data')).get(event_type, 'Any')
