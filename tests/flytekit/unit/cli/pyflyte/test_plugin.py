from unittest.mock import patch, Mock

from flytekit.clis.sdk_in_container.plugin import get_cli_plugin, PyFlyteCLIPlugin


@patch("flytekit.clis.sdk_in_container.plugin.entry_points")
def test_get_plugin_default(entry_points):
    entry_points.side_effect = lambda *args, **kwargs: []

    default_plugin = get_cli_plugin()
    assert default_plugin is PyFlyteCLIPlugin


@patch("flytekit.clis.sdk_in_container.plugin.entry_points")
def test_get_plugin_load_other_plugin(entry_points, caplog):
    loaded_plugin_1 = Mock()
    entry_1 = Mock()
    entry_1.name = "entry_1"
    entry_1.load.side_effect = lambda: loaded_plugin_1

    entry_2 = Mock()
    entry_points.side_effect = lambda *args, **kwargs: [entry_1, entry_2]

    plugin = get_cli_plugin()
    assert plugin is loaded_plugin_1

    assert entry_1.load.call_count == 1
    assert entry_2.load.call_count == 0
