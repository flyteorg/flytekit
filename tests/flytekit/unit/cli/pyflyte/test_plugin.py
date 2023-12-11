from unittest.mock import patch, Mock
import click

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


class CustomPlugin(PyFlyteCLIPlugin):
    @staticmethod
    def configure_pyflyte_cli(main):
        """Make config hidden in main CLI."""
        for p in main.params:
            if p.name == "config":
                p.hidden = True
        return main


@click.command
@click.option(
    "-c",
    "--config",
    required=False,
    type=str,
    help="Path to config file for use within container",
)
def click_main(config):
    pass


@patch("flytekit.clis.sdk_in_container.plugin.entry_points")
def test_get_plugin_custom(entry_points):
    entry_1 = Mock()
    entry_1.name.side_effect = "custom_plugin"
    entry_1.load.side_effect = lambda: CustomPlugin

    entry_points.side_effect = lambda *args, **kwargs: [entry_1]

    plugin = get_cli_plugin()
    assert plugin is CustomPlugin

    assert not click_main.params[0].hidden

    plugin.configure_pyflyte_cli(click_main)
    assert click_main.params[0].hidden
