from __future__ import annotations

import typing

import click

from flytekit.core.context_manager import FlyteContext
from flytekit.core.type_engine import TypeEngine
from flytekit.models.literals import Literal


def parse_stdin_to_literal(ctx: FlyteContext, t: typing.Type, message: typing.Optional[str]) -> Literal:
    """
    Parses the user input from stdin and converts it to a literal of the given type.
    """
    from flytekit.interaction.click_types import FlyteLiteralConverter

    literal_type = TypeEngine.to_literal_type(t)
    literal_converter = FlyteLiteralConverter(
        ctx,
        literal_type=literal_type,
        python_type=t,
        get_upload_url_fn=lambda: None,
        is_remote=False,
        remote_instance_accessor=None,
    )
    user_input = click.prompt(message, type=literal_converter.click_type)
    return literal_converter.convert_to_literal(click.Context(click.Command(None)), None, user_input)
