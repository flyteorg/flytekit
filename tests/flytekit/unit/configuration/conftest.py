from __future__ import absolute_import
from flytekit.configuration import set_flyte_config_file as _set_config
import pytest as _pytest


@_pytest.fixture(scope="function", autouse=True)
def clear_configs():
    _set_config(None)
    yield
    _set_config(None)
