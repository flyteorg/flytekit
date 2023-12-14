import pytest

import flytekit.clis.sdk_in_container.helpers
import flytekit.clis.sdk_in_container.pyflyte
from flytekit.configuration.plugin import FlytekitPlugin


@pytest.fixture(autouse=True, scope="session")
def configure_plugin():
    """If a plugin is installed then the plugin variable points to a external plugin.
    For testing, we want to test against flytekit's own plugin, so we override the plugins."""
    flytekit.configuration.plugin.plugin = FlytekitPlugin
    flytekit.clis.sdk_in_container.pyflyte.plugin = FlytekitPlugin
    flytekit.clis.sdk_in_container.helpers.plugin = FlytekitPlugin
