from __future__ import absolute_import
from flytekit.configuration import TemporaryConfiguration
import pytest as _pytest


@_pytest.fixture(scope='function', autouse=True)
def set_fake_config():
    with TemporaryConfiguration(None, internal_overrides={'image': 'fakeimage'}):
        yield
