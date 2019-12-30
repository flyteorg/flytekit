from __future__ import absolute_import
from flytekit.configuration import set_flyte_config_file as _set_config
import pytest as _pytest
import os as _os

@_pytest.fixture(scope="function", autouse=True)
def clear_configs():
    _set_config(None)
    environment_variables = _os.environ.copy()
    yield
    _os.environ = environment_variables
    _set_config(None)
