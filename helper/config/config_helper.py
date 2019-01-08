"""
Class Spark Helper

@author Irfan Andriansyah <irfan@99.co>

Documentation:
https://docs.python.org/3/library/configparser.html
"""

import os
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
