import os
import types
import typing
from dataclasses import Field, dataclass, field
from types import MappingProxyType

import grpc
import rich_click as click
from rich.console import Console
from rich.panel import Panel
from rich.syntax import Syntax
from rich.traceback import Traceback

from flytekit.core.constants import SOURCE_CODE
from flytekit.exceptions.base import FlyteException
from flytekit.exceptions.user import FlyteCompilationException, FlyteInvalidInputException
from flytekit.exceptions.utils import annotate_exception_with_code
from flytekit.loggers import get_level_from_cli_verbosity, logger

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
    logger.debug(f"Using packages: {pkgs}")
    return pkgs


def pretty_print_grpc_error(e: grpc.RpcError):
    """
    This method will print the grpc error that us more human readable.
    """
    if isinstance(e, grpc._channel._InactiveRpcError):  # noqa
        click.secho(f"RPC Failed, with Status: {e.code()}", fg="red", bold=True)
        click.secho(f"\tDetails: {e.details()}", fg="magenta", bold=True)
    return


def remove_unwanted_traceback_frames(
    tb: types.TracebackType, unwanted_module_names: typing.List[str]
) -> types.TracebackType:
    """
    Custom function to remove certain frames from the traceback.
    """
    frames = []
    while tb is not None:
        frame = tb.tb_frame
        frame_info = (frame.f_code.co_filename, frame.f_code.co_name, frame.f_lineno)
        if not any(module_name in frame_info[0] for module_name in unwanted_module_names):
            frames.append((frame, tb.tb_lasti, tb.tb_lineno))
        tb = tb.tb_next

    # Recreate the traceback without unwanted frames
    tb_next = None
    for frame, tb_lasti, tb_lineno in reversed(frames):
        tb_next = types.TracebackType(tb_next, frame, tb_lasti, tb_lineno)

    return tb_next


def pretty_print_traceback(e: Exception, verbosity: int = 1):
    """
    This method will print the Traceback of an error.
    Print the traceback in a nice formatted way if verbose is set to True.
    """
    console = Console()
    unwanted_module_names = ["importlib", "click", "rich_click"]

    if verbosity == 0:
        unwanted_module_names.append("flytekit")
        tb = e.__cause__.__traceback__ if e.__cause__ else e.__traceback__
        new_tb = remove_unwanted_traceback_frames(tb, unwanted_module_names)
        console.print(Traceback.from_exception(type(e), e, new_tb))
    elif verbosity == 1:
        click.secho(
            f"Frames from the following modules were removed from the traceback: {unwanted_module_names}."
            f" For more verbose output, use the flags -vv or -vvv.",
            fg="yellow",
        )

        new_tb = remove_unwanted_traceback_frames(e.__traceback__, unwanted_module_names)
        console.print(Traceback.from_exception(type(e), e, new_tb))
    elif verbosity >= 2:
        console.print(Traceback.from_exception(type(e), e, e.__traceback__))
    else:
        raise ValueError(f"Verbosity level must be between 0 and 2. Got {verbosity}")

    if isinstance(e, FlyteCompilationException):
        e = annotate_exception_with_code(e, e.fn, e.param_name)
        if hasattr(e, SOURCE_CODE):
            # TODO: Use other way to check if the background is light or dark
            theme = "emacs" if "LIGHT_BACKGROUND" in os.environ else "monokai"
            syntax = Syntax(getattr(e, SOURCE_CODE), "python", theme=theme, background_color="default")
            panel = Panel(syntax, border_style="red", title=e._ERROR_CODE, title_align="left")
            console.print(panel, no_wrap=False)


def pretty_print_exception(e: Exception, verbosity: int = 1):
    """
    This method will print the exception in a nice way. It will also check if the exception is a grpc.RpcError and
    print it in a human-readable way.
    """
    if verbosity > 0:
        click.secho("Verbose mode on")

    if isinstance(e, click.exceptions.Exit):
        raise e

    if isinstance(e, click.ClickException):
        raise e

    if isinstance(e, FlyteException):
        if isinstance(e, FlyteInvalidInputException):
            click.secho("Request rejected by the API, due to Invalid input.", fg="red")
        cause = e.__cause__
        if cause:
            if isinstance(cause, grpc.RpcError):
                pretty_print_grpc_error(cause)
            else:
                pretty_print_traceback(e, verbosity)
        else:
            pretty_print_traceback(e, verbosity)
        return

    if isinstance(e, grpc.RpcError):
        pretty_print_grpc_error(e)
        return

    pretty_print_traceback(e, verbosity)


class ErrorHandlingCommand(click.RichGroup):
    """
    Helper class that wraps the invoke method of a click command to catch exceptions and print them in a nice way.
    """

    def invoke(self, ctx: click.Context) -> typing.Any:
        verbosity = ctx.params["verbose"]
        log_level = get_level_from_cli_verbosity(verbosity)
        logger.setLevel(log_level)
        try:
            return super().invoke(ctx)
        except Exception as e:
            pretty_print_exception(e, verbosity)
            exit(1)


def make_click_option_field(o: click.Option) -> Field:
    if o.multiple:
        o.help = click.style("Multiple values allowed.", bold=True) + f"{o.help}"
        return field(default_factory=lambda: o.default, metadata={"click.option": o})
    return field(default=o.default, metadata={"click.option": o})


def get_option_from_metadata(metadata: MappingProxyType) -> click.Option:
    return metadata["click.option"]


@dataclass
class PyFlyteParams:
    config_file: typing.Optional[str] = None
    verbose: bool = False
    pkgs: typing.List[str] = field(default_factory=list)

    @classmethod
    def from_dict(cls, d: typing.Dict[str, typing.Any]) -> "PyFlyteParams":
        return cls(**d)
