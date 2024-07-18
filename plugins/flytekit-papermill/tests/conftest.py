import os

import pytest


@pytest.fixture(scope="module", autouse=True)
def set_default_envs():
    os.environ["FLYTE_SDK_RICH_TRACEBACKS"] = "0"
