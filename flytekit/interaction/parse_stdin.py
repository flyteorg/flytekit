from __future__ import annotations

import typing

import click

from flytekit.core.type_engine import TypeEngine
from flytekit.loggers import logger
from flytekit.models.literals import Literal
from flytekit.core.context_manager import FlyteContext


# TODO: Move the improved click parsing here. https://github.com/flyteorg/flyte/issues/3124
def parse_stdin_to_literal(ctx: FlyteContext, t: typing.Type, message: typing.Optional[str]) -> Literal:

    if message:
        click.secho(message, bold=True, fg="yellow")
    user_input = input("")
    if issubclass(t, bool):
        l = TypeEngine.to_literal(ctx, True, bool, TypeEngine.to_literal_type(bool))  # noqa
    elif issubclass(t, int):
        ii = int(user_input)
        l = TypeEngine.to_literal(ctx, ii, int, TypeEngine.to_literal_type(int))  # noqa
    elif issubclass(t, float):
        ff = float(user_input)
        l = TypeEngine.to_literal(ctx, ff, float, TypeEngine.to_literal_type(float))  # noqa
    elif issubclass(t, str):
        ss = str(user_input)
        l = TypeEngine.to_literal(ctx, ss, str, TypeEngine.to_literal_type(str))  # noqa
    else:
        # Todo: We should implement the rest by way of importing the code in pyflyte run
        #   that parses text from the command line
        raise Exception("Only bool, int/float, or strings are accepted for now.")

    logger.debug(f"Parsed literal {l} from user input {user_input}")
    return l