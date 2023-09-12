import typing

import grpc
import rich_click as click
from google.protobuf.json_format import MessageToJson

from flytekit.clis.sdk_in_container.constants import CTX_VERBOSE
from flytekit.exceptions.base import FlyteException
from flytekit.exceptions.user import FlyteInvalidInputException
from flytekit.loggers import cli_logger


def validate_package(ctx, param, values):
    """
    This method will validate the packages passed in by the user. It will check that the packages are in the correct
    format, and will also split the packages if the user passed in a comma separated list.
    """
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
    """
    This method will print the grpc error that us more human readable.
    """
    if isinstance(e, grpc._channel._InactiveRpcError):  # noqa
        click.secho(f"RPC Failed, with Status: {e.code()}", fg="red", bold=True)
        click.secho(f"\tdetails: {e.details()}", fg="magenta", bold=True)
        click.secho(f"\tDebug string {e.debug_error_string()}", dim=True)
    return


def pretty_print_exception(e: Exception):
    """
    This method will print the exception in a nice way. It will also check if the exception is a grpc.RpcError and
    print it in a human-readable way.
    """
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
    """
    Helper class that wraps the invoke method of a click command to catch exceptions and print them in a nice way.
    """

    def invoke(self, ctx: click.Context) -> typing.Any:
        try:
            return super().invoke(ctx)
        except Exception as e:
            if CTX_VERBOSE in ctx.obj and ctx.obj[CTX_VERBOSE]:
                click.secho("Verbose mode on")
                raise e
            pretty_print_exception(e)
            raise SystemExit(e) from e
