from typing import Optional
import sys

from click import Command
from importlib_metadata import entry_points
from flytekit.configuration import Config, get_config_file
from flytekit.loggers import cli_logger
from flytekit.remote import FlyteRemote


class PyFlyteCLIPlugin:
    @staticmethod
    def get_remote(
        config: Optional[str], project: str, domain: str, data_upload_location: Optional[str] = None
    ) -> FlyteRemote:
        """Get FlyteRemote object for CLI session."""
        cfg_file = get_config_file(config)
        if cfg_file is None:
            cfg_obj = Config.for_sandbox()
            cli_logger.info("No config files found, creating remote with sandbox config")
        else:
            cfg_obj = Config.auto(config)
            cli_logger.info(f"Creating remote with config {cfg_obj}" + (f" with file {config}" if config else ""))
        return FlyteRemote(
            cfg_obj, default_project=project, default_domain=domain, data_upload_location=data_upload_location
        )

    @staticmethod
    def configure_pyflyte_cli(main: Command) -> Command:
        """Configure pyflyte's CLI."""
        return main


def get_cli_plugin():
    """Get plugin for entrypoint."""
    cli_plugins = list(entry_points(group="flytekit.cli.plugin"))

    if not cli_plugins:
        return PyFlyteCLIPlugin

    if len(cli_plugins) >= 2:
        plugin_names = [p.name for p in cli_plugins]
        cli_logger.info(f"Multiple plugins seen for flytekit.cli.plugin: {plugin_names}")

    cli_plugin_to_load = cli_plugins[0]
    cli_logger.info(f"Loading plugin: {cli_plugin_to_load.name}")
    return cli_plugin_to_load.load()


# Ensure that cli_plugin is always configured to PyFlyteCLIPlugin during pytest runs
if "pytest" in sys.modules:
    cli_plugin = PyFlyteCLIPlugin
else:
    cli_plugin = get_cli_plugin()
