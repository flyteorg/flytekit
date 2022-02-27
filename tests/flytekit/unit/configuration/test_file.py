import configparser
import os

import pytest

from flytekit.configuration import file, set_if_exists, get_config_file


def test_set_if_exists():
    d = {}
    d = set_if_exists(d, "k", None)
    assert len(d) == 0
    d = set_if_exists(d, "k", [])
    assert len(d) == 0
    d = set_if_exists(d, "k", 1)
    assert len(d) == 1


def test_get_config_file():
    c = get_config_file(None)
    assert c is None
    c = get_config_file(os.path.join(os.path.dirname(os.path.realpath(__file__)), "configs/good.config"))
    assert c is not None
    assert c.legacy_config is not None

    with pytest.raises(configparser.Error):
        get_config_file(os.path.join(os.path.dirname(os.path.realpath(__file__)), "configs/bad.config"))
