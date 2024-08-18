import os

import pytest

import flytekit.configuration.plugin
from flytekit.configuration.plugin import FlytekitPlugin


@pytest.fixture(autouse=True, scope="session")
def configure_plugin():
    """If a plugin is installed then the global plugin refers to an external plugin.
    For testing, we want to test against flytekit's own plugin, so we override the state."""
    flytekit.configuration.plugin._GLOBAL_CONFIG["plugin"] = FlytekitPlugin


@pytest.fixture(scope="module", autouse=True)
def set_default_envs():
    os.environ["FLYTE_EXIT_ON_USER_EXCEPTION"] = "0"
