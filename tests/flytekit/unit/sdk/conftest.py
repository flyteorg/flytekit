from __future__ import absolute_import

import pytest as _pytest

from flytekit.configuration import TemporaryConfiguration


@_pytest.fixture(scope="function", autouse=True)
def set_fake_config():
    with TemporaryConfiguration(None, internal_overrides={"image": "fakeimage"}):
        yield
