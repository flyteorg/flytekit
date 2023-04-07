from __future__ import annotations

import typing
from dataclasses import dataclass

from dataclasses_json import dataclass_json
from typing_extensions import Annotated  # type: ignore

from flytekit.core import context_manager
from flytekit.core.interface import transform_function_to_interface, transform_inputs_to_parameters


@dataclass_json
@dataclass
class Foo(object):
    x: int
    y: str
    z: typing.Dict[str, str]


def test_structured_dataset():
    ctx = context_manager.FlyteContext.current_context()

    def z(a: Annotated[int, "some annotation"]) -> Annotated[int, "some annotation"]:
        return a

    our_interface = transform_function_to_interface(z)
    params = transform_inputs_to_parameters(ctx, our_interface)
    assert params.parameters["a"].required
    assert params.parameters["a"].default is None
    assert our_interface.inputs == {"a": Annotated[int, "some annotation"]}
    assert our_interface.outputs == {"o0": Annotated[int, "some annotation"]}
