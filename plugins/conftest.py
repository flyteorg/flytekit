import os

import pytest


@pytest.fixture(scope="module", autouse=True)
def set_default_envs():
    os.environ["FLYTE_EXIT_ON_USER_EXCEPTION"] = "0"
