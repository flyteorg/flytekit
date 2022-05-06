import click

from flytekit import configuration
from flytekit.clis.sdk_in_container.constants import CTX_CONFIG_FILE, CTX_PACKAGES
from flytekit.clis.sdk_in_container.init import init
from flytekit.clis.sdk_in_container.local_cache import local_cache
from flytekit.clis.sdk_in_container.package import package
from flytekit.clis.sdk_in_container.run import run
from flytekit.clis.sdk_in_container.serialize import serialize
from flytekit.configuration.internal import LocalSDK


def validate_package(ctx, param, values):
    for val in values:
        if "/" in val or "-" in val or "\\" in val:
            raise click.BadParameter(
                f"Illegal package value {val} for parameter: {param}. Expected for the form [a.b.c]"
            )
    return values


@click.group("pyflyte", invoke_without_command=True)
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
    "-c",
    "--config",
    required=False,
    type=str,
    help="Path to config file for use within container",
)
@click.pass_context
def main(ctx, pkgs=None, config=None):
    """
    Entrypoint for all the user commands.
    """
    ctx.obj = dict()

    # Handle package management - get from config if not specified on the command line
    pkgs = pkgs or []
    if config:
        ctx.obj[CTX_CONFIG_FILE] = config
        cfg = configuration.ConfigFile(config)
        if not pkgs:
            pkgs = LocalSDK.WORKFLOW_PACKAGES.read(cfg)
            if pkgs is None:
                pkgs = []
    ctx.obj[CTX_PACKAGES] = pkgs


main.add_command(serialize)
main.add_command(package)
main.add_command(local_cache)
main.add_command(init)
main.add_command(run)

if __name__ == "__main__":
    main()
