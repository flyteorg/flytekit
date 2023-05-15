import typing

import grpc
import rich_click as click
from google.protobuf.json_format import MessageToJson

from flytekit import configuration
from flytekit.clis.sdk_in_container.backfill import backfill
from flytekit.clis.sdk_in_container.build import build
from flytekit.clis.sdk_in_container.constants import CTX_CONFIG_FILE, CTX_PACKAGES, CTX_VERBOSE
from flytekit.clis.sdk_in_container.init import init
from flytekit.clis.sdk_in_container.launchplan import launchplan
from flytekit.clis.sdk_in_container.local_cache import local_cache
from flytekit.clis.sdk_in_container.metrics import metrics
from flytekit.clis.sdk_in_container.package import package
from flytekit.clis.sdk_in_container.register import register
from flytekit.clis.sdk_in_container.run import run
from flytekit.clis.sdk_in_container.serialize import serialize
from flytekit.clis.sdk_in_container.serve import serve
from flytekit.configuration.internal import LocalSDK
from flytekit.exceptions.base import FlyteException
from flytekit.exceptions.user import FlyteInvalidInputException
from flytekit.loggers import cli_logger


def validate_package(ctx, param, values):
    pkgs = []
    for val in values:
        if "/" in val or "-" in val or "\\" in val:
            raise click.BadParameter(
                f"Illegal package value {val} for parameter: {param}. Expected for the form [a.b.c]"
            )
        elif "," in val:
            pkgs.extend(val.split(","))
        else:
            pkgs.append(val)
    cli_logger.debug(f"Using packages: {pkgs}")
    return pkgs


def pretty_print_grpc_error(e: grpc.RpcError):
    if isinstance(e, grpc._channel._InactiveRpcError):  # noqa
        click.secho(f"RPC Failed, with Status: {e.code()}", fg="red", bold=True)
        click.secho(f"\tdetails: {e.details()}", fg="magenta", bold=True)
        click.secho(f"\tDebug string {e.debug_error_string()}", dim=True)
    return


def pretty_print_exception(e: Exception):
    if isinstance(e, click.exceptions.Exit):
        raise e

    if isinstance(e, click.ClickException):
        click.secho(e.message, fg="red")
        raise e

    if isinstance(e, FlyteException):
        click.secho(f"Failed with Exception Code: {e._ERROR_CODE}", fg="red")  # noqa
        if isinstance(e, FlyteInvalidInputException):
            click.secho("Request rejected by the API, due to Invalid input.", fg="red")
            click.secho(f"\tInput Request: {MessageToJson(e.request)}", dim=True)

        cause = e.__cause__
        if cause:
            if isinstance(cause, grpc.RpcError):
                pretty_print_grpc_error(cause)
            else:
                click.secho(f"Underlying Exception: {cause}")
        return

    if isinstance(e, grpc.RpcError):
        pretty_print_grpc_error(e)
        return

    click.secho(f"Failed with Unknown Exception {type(e)} Reason: {e}", fg="red")  # noqa


class ErrorHandlingCommand(click.RichGroup):
    def invoke(self, ctx: click.Context) -> typing.Any:
        try:
            return super().invoke(ctx)
        except Exception as e:
            if CTX_VERBOSE in ctx.obj and ctx.obj[CTX_VERBOSE]:
                print("Verbose mode on")
                raise e
            pretty_print_exception(e)
            raise SystemExit(e)


@click.group("pyflyte", invoke_without_command=True, cls=ErrorHandlingCommand)
@click.option(
    "--verbose", required=False, default=False, is_flag=True, help="Show verbose messages and exception traces"
)
@click.option(
    "-k",
    "--pkgs",
    required=False,
    multiple=True,
    callback=validate_package,
    help="Dot-delineated python packages to operate on. Multiple may be specified (can use commas, or specify the "
    "switch multiple times. Please note that this "
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
def main(ctx, pkgs: typing.List[str], config: str, verbose: bool):
    """
    Entrypoint for all the user commands.
    """
    ctx.obj = dict()

    # Handle package management - get from the command line, the environment variables, then the config file.
    pkgs = pkgs or LocalSDK.WORKFLOW_PACKAGES.read() or []
    if config:
        ctx.obj[CTX_CONFIG_FILE] = config
        cfg = configuration.ConfigFile(config)
        if not pkgs:
            pkgs = LocalSDK.WORKFLOW_PACKAGES.read(cfg)
            if pkgs is None:
                pkgs = []
    ctx.obj[CTX_PACKAGES] = pkgs
    ctx.obj[CTX_VERBOSE] = verbose


main.add_command(serialize)
main.add_command(package)
main.add_command(local_cache)
main.add_command(init)
main.add_command(run)
main.add_command(register)
main.add_command(backfill)
main.add_command(serve)
main.add_command(build)
main.add_command(metrics)
main.add_command(launchplan)
main.epilog

if __name__ == "__main__":
    main()
