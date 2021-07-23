import logging as _logging
import os as _os
from pathlib import Path

import click

from flytekit.clis.sdk_in_container.constants import CTX_PACKAGES
from flytekit.clis.sdk_in_container.fast_register import fast_register
from flytekit.clis.sdk_in_container.launch_plan import launch_plans
from flytekit.clis.sdk_in_container.package import package
from flytekit.clis.sdk_in_container.register import register
from flytekit.clis.sdk_in_container.serialize import serialize
from flytekit.configuration import internal as _internal_config
from flytekit.configuration import platform as _platform_config
from flytekit.configuration import sdk as _sdk_config
from flytekit.configuration import set_flyte_config_file
from flytekit.configuration.internal import CONFIGURATION_PATH
from flytekit.configuration.platform import URL as _URL
from flytekit.configuration.sdk import WORKFLOW_PACKAGES as _WORKFLOW_PACKAGES


def validate_package(ctx, param, values):
    for val in values:
        if "/" in val or "-" in val or "\\" in val:
            raise click.BadParameter(
                f"Illegal package value {val} for parameter: {param}. Expected for the form [a.b.c]"
            )
    return values


@click.group("pyflyte", invoke_without_command=True)
@click.option(
    "-c",
    "--config",
    required=False,
    type=str,
    help="Path to config file for use within container",
)
@click.option(
    "-k",
    "--pkgs",
    required=False,
    multiple=True,
    callback=validate_package,
    help="Dot separated python packages to operate on.  Multiple may be specified  Please note that this "
    "option will override the option specified in the configuration file, or environment variable",
)
@click.option(
    "-i",
    "--insecure",
    required=False,
    type=bool,
    help="Disable SSL when connecting to Flyte backend.",
)
@click.pass_context
def main(ctx, config=None, pkgs=None, insecure=None):
    """
    Entrypoint for all the user commands.
    """
    update_configuration_file(config)

    # Update the logger if it's set
    log_level = _internal_config.LOGGING_LEVEL.get() or _sdk_config.LOGGING_LEVEL.get()
    if log_level is not None:
        _logging.getLogger().setLevel(log_level)

    ctx.obj = dict()

    # Determine SSL.  Note that the insecure option in this command is not a flag because we want to look
    # up configuration settings if it's missing.  If the command line option says insecure but the config object
    # says no, let's override the config object by overriding the environment variable.
    if insecure and not _platform_config.INSECURE.get():
        _platform_config.INSECURE.get()
        _os.environ[_platform_config.INSECURE.env_var] = "True"

    # Handle package management - get from config if not specified on the command line
    pkgs = pkgs or []
    if len(pkgs) == 0:
        pkgs = _WORKFLOW_PACKAGES.get()
    ctx.obj[CTX_PACKAGES] = pkgs


def update_configuration_file(config_file_path):
    """
    Changes the configuration singleton object to read from another file if specified, which should be
    at the base of the repository.

    :param Text config_file_path:
    """
    configuration_file = Path(config_file_path or CONFIGURATION_PATH.get())
    if configuration_file.is_file():
        click.secho(
            "Using configuration file at {}".format(configuration_file.absolute().as_posix()),
            fg="green",
        )
        set_flyte_config_file(configuration_file.as_posix())
    else:
        click.secho(
            "Configuration file '{}' could not be loaded. Using values from environment.".format(
                CONFIGURATION_PATH.get()
            ),
            color="yellow",
        )
        set_flyte_config_file(None)
    click.secho("Flyte Admin URL {}".format(_URL.get()), fg="green")


main.add_command(register)
main.add_command(fast_register)
main.add_command(serialize)
main.add_command(launch_plans)
main.add_command(package)

if __name__ == "__main__":
    main()
