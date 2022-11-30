from __future__ import annotations

import typing

import click

from flytekit.core.context_manager import FlyteContext
from flytekit.core.type_engine import TypeEngine
from flytekit.loggers import logger
from flytekit.models.literals import Literal


# TODO: Move the improved click parsing here. https://github.com/flyteorg/flyte/issues/3124
def parse_stdin_to_literal(ctx: FlyteContext, t: typing.Type, message_prefix: typing.Optional[str]) -> Literal:

    message = message_prefix or ""
    message += f"Please enter value for type {t} to continue"
    if issubclass(t, bool):
        user_input = click.prompt(message, type=bool)
        l = TypeEngine.to_literal(ctx, user_input, bool, TypeEngine.to_literal_type(bool))  # noqa
    elif issubclass(t, int):
        user_input = click.prompt(message, type=int)
        l = TypeEngine.to_literal(ctx, user_input, int, TypeEngine.to_literal_type(int))  # noqa
    elif issubclass(t, float):
        user_input = click.prompt(message, type=float)
        l = TypeEngine.to_literal(ctx, user_input, float, TypeEngine.to_literal_type(float))  # noqa
    elif issubclass(t, str):
        user_input = click.prompt(message, type=str)
        l = TypeEngine.to_literal(ctx, user_input, str, TypeEngine.to_literal_type(str))  # noqa
    else:
        # Todo: We should implement the rest by way of importing the code in pyflyte run
        #   that parses text from the command line
        raise Exception("Only bool, int/float, or strings are accepted for now.")

    logger.debug(f"Parsed literal {l} from user input {user_input}")
    return l
