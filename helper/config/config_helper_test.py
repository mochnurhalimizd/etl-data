"""
Config Helper Test Case

@author Irfan Andriansyah <irfan@99.co>
"""
import pytest
from helper.config.config_helper import get_config


def test_get_config():
    """
    test case for method get_config
    """

    config = get_config('helper/config/config.test.conf')
    assert config.get('test', 'message') == 'ini testing'

    with pytest.raises(Exception):
        config.get('test', 'sasa')
