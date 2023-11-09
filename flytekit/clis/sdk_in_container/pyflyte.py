import os
import typing

import rich_click as click

from flytekit import configuration
from flytekit.clis.sdk_in_container.backfill import backfill
from flytekit.clis.sdk_in_container.build import build
from flytekit.clis.sdk_in_container.fetch import fetch
from flytekit.clis.sdk_in_container.get import get
from flytekit.clis.sdk_in_container.init import init
from flytekit.clis.sdk_in_container.launchplan import launchplan
from flytekit.clis.sdk_in_container.local_cache import local_cache
from flytekit.clis.sdk_in_container.metrics import metrics
from flytekit.clis.sdk_in_container.package import package
from flytekit.clis.sdk_in_container.register import register
from flytekit.clis.sdk_in_container.run import run
from flytekit.clis.sdk_in_container.serialize import serialize
from flytekit.clis.sdk_in_container.serve import serve
from flytekit.clis.sdk_in_container.utils import pretty_print_exception, BaseOptions, pass_base_opts
from flytekit.clis.version import info
from flytekit.configuration.file import FLYTECTL_CONFIG_ENV_VAR, FLYTECTL_CONFIG_ENV_VAR_OVERRIDE
from flytekit.configuration.internal import LocalSDK
from flytekit.loggers import cli_logger


class BaseCommand(click.RichGroup):
    """
    The base pyflyte command group that nests all the other commands.
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, params=BaseOptions.options(), **kwargs)

    def _wrangle_config_file(self, config: typing.Optional[str], pkgs: typing.List[str] = None):
        if config:
            cfg = configuration.ConfigFile(config)
            # Set here so that if someone has Config.auto() in their user code, the config here will get used.
            if FLYTECTL_CONFIG_ENV_VAR in os.environ:
                cli_logger.info(
                    f"Config file arg {config} will override env var {FLYTECTL_CONFIG_ENV_VAR}: {os.environ[FLYTECTL_CONFIG_ENV_VAR]}"
                )
            os.environ[FLYTECTL_CONFIG_ENV_VAR_OVERRIDE] = config
            if not pkgs:
                pkgs = LocalSDK.WORKFLOW_PACKAGES.read(cfg)
                if pkgs is None:
                    return []
        return pkgs

    def invoke(self, ctx: click.Context) -> typing.Any:
        base_opts = BaseOptions.from_dict(ctx.params)
        pkgs = base_opts.pkgs or LocalSDK.WORKFLOW_PACKAGES.read() or []
        base_opts.pkgs = self._wrangle_config_file(base_opts.config, pkgs)
        ctx.obj = base_opts
        try:
            return super().invoke(ctx)
        except click.exceptions.UsageError:
            raise
        except Exception as e:
            if base_opts.verbose:
                click.secho("Verbose mode on")
                raise e
            pretty_print_exception(e)
            raise SystemExit(e) from e


@pass_base_opts
def main_cb(opts: BaseOptions, *args, **kwargs):
    pass


main = BaseCommand("pyflyte", invoke_without_command=True, callback=main_cb)


def register_subcommand(cmd: click.Command, override_existing: bool = False):
    """
    This method is used to register a subcommand with the pyflyte group. This is useful for plugins that want to add
    their own subcommands to the pyflyte group. This method should be called from the plugin's entrypoint.
    """
    if main.get_command(None, cmd.name) is not None and not override_existing:
        raise ValueError(f"Command {cmd.name} already registered. Skipping")
    cli_logger.info(f"Registering command {cmd.name}")
    main.add_command(cmd)


def unregister_subcommand(name: str):
    """
    This method is used to unregister a subcommand with the pyflyte group. This is useful for plugins that want to
    remove a subcommand from the pyflyte group. This method should be called from the plugin's entrypoint.
    """
    if main.get_command(None, name) is None:
        return
    cli_logger.info(f"Unregistering command {name}")
    main.commands.pop(name)


register_subcommand(serialize)
register_subcommand(package)
register_subcommand(local_cache)
register_subcommand(init)
register_subcommand(run)
register_subcommand(register)
register_subcommand(backfill)
register_subcommand(serve)
register_subcommand(build)
register_subcommand(metrics)
register_subcommand(launchplan)
register_subcommand(fetch)
register_subcommand(info)
register_subcommand(get)

if __name__ == "__main__":
    main()
