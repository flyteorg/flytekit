import os
import typing
from dataclasses import Field, dataclass, field, fields
from types import MappingProxyType

import grpc
import rich_click as click
from google.protobuf.json_format import MessageToJson

from flytekit.clis.sdk_in_container.constants import CTX_VERBOSE
from flytekit.configuration import Config
from flytekit.configuration.file import FLYTECTL_CONFIG_ENV_VAR, get_config_file
from flytekit.exceptions.base import FlyteException
from flytekit.exceptions.user import FlyteInvalidInputException
from flytekit.loggers import cli_logger
from flytekit.remote import FlyteRemote

project_option = click.Option(
    param_decls=["-p", "--project"],
    required=False,
    type=str,
    default=os.getenv("FLYTE_DEFAULT_PROJECT", "flytesnacks"),
    show_default=True,
    help="Project to register and run this workflow in. Can also be set through envvar " "``FLYTE_DEFAULT_PROJECT``",
)

domain_option = click.Option(
    param_decls=["-d", "--domain"],
    required=False,
    type=str,
    default=os.getenv("FLYTE_DEFAULT_DOMAIN", "development"),
    show_default=True,
    help="Domain to register and run this workflow in, can also be set through envvar " "``FLYTE_DEFAULT_DOMAIN``",
)

project_option_dec = click.option(
    "-p",
    "--project",
    required=False,
    type=str,
    default=os.getenv("FLYTE_DEFAULT_PROJECT", "flytesnacks"),
    show_default=True,
    help="Project for workflow/launchplan. Can also be set through envvar " "``FLYTE_DEFAULT_PROJECT``",
)

domain_option_dec = click.option(
    "-d",
    "--domain",
    required=False,
    type=str,
    default=os.getenv("FLYTE_DEFAULT_DOMAIN", "development"),
    show_default=True,
    help="Domain for workflow/launchplan, can also be set through envvar " "``FLYTE_DEFAULT_DOMAIN``",
)


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
        except click.exceptions.UsageError:
            raise
        except Exception as e:
            if CTX_VERBOSE in ctx.obj and ctx.obj[CTX_VERBOSE]:
                click.secho("Verbose mode on")
                raise e
            pretty_print_exception(e)
            raise SystemExit(e) from e


def make_click_option_field(o: click.Option) -> Field:
    if o.multiple:
        o.help = click.style("Multiple values allowed.", bold=True) + f"{o.help}"
        return field(default_factory=lambda: o.default, metadata={"click.option": o})
    return field(default=o.default, metadata={"click.option": o})


def get_option_from_metadata(metadata: MappingProxyType) -> click.Option:
    return metadata["click.option"]


T = typing.TypeVar("T")


class ClickOptionsMixin(typing.Generic[T]):
    """
    This mixin can be added to any dataclass to add a from_dict method that will take a dictionary of parameters and
    convert them to the correct dataclass and analyze the metadata for all fields of the dataclass convert them to
    click options.
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    @classmethod
    def from_dict(cls, d: typing.Dict[str, typing.Any]) -> T:
        return cls(**d)

    @classmethod
    def options(cls) -> typing.List[click.Option]:
        """
        Return the set of base parameters added to every pyflyte run workflow subcommand.
        """
        return [get_option_from_metadata(f.metadata) for f in fields(cls) if f.metadata]


@dataclass
class PyFlyteParams:
    config_file: typing.Optional[str] = None
    verbose: bool = False
    pkgs: typing.List[str] = field(default_factory=list)

    @classmethod
    def from_dict(cls, d: typing.Dict[str, typing.Any]) -> "PyFlyteParams":
        return cls(**d)


@dataclass
class BaseOptions(ClickOptionsMixin["BaseOptions"]):
    verbose: bool = make_click_option_field(
        click.Option(
            param_decls=["--verbose", "-v"],
            required=False,
            default=False,
            is_flag=True,
            help="Show verbose messages and exception traces",
        )
    )
    pkgs: typing.List[str] = make_click_option_field(
        click.Option(
            param_decls=["-k", "--pkgs"],
            required=False,
            multiple=True,
            callback=validate_package,
            help="Dot-delineated python packages to operate on. Multiple may be specified (can use commas, or specify "
            "the switch multiple times. Please note that this option will override the option specified in the "
            "configuration file, or environment variable",
        )
    )
    config: typing.Optional[str] = make_click_option_field(
        click.Option(
            param_decls=["-c", "--config"],
            required=False,
            type=str,
            envvar=FLYTECTL_CONFIG_ENV_VAR,
            help="Path to config file for use within container",
        )
    )

    def load_config(self) -> Config:
        cfg_file = get_config_file(self.config)
        if cfg_file is None:
            cfg_obj = Config.for_sandbox()
            cli_logger.info("No config files found, creating remote with sandbox config")
        else:
            cfg_obj = Config.auto(self.config)
            cli_logger.info(
                f"Creating remote with config {cfg_obj}" + (f" with file {self.config}" if self.config else "")
            )
        return cfg_obj

    def get_remote(
        self,
        project: typing.Optional[str] = None,
        domain: typing.Optional[str] = None,
        data_upload_location: typing.Optional[str] = None,
    ):
        cfg_obj = self.load_config()
        return FlyteRemote(
            cfg_obj, default_project=project, default_domain=domain, data_upload_location=data_upload_location
        )


pass_base_opts = click.make_pass_decorator(BaseOptions)
