import os

import pytest

import flytekit.configuration.plugin
from flytekit.configuration.plugin import FlytekitPlugin


@pytest.fixture(scope="module", autouse=True)
def set_default_envs():
    os.environ["FLYTE_SDK_RICH_TRACEBACKS"] = "0"
