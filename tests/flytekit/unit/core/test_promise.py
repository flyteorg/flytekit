import typing
from dataclasses import dataclass

import pytest
from dataclasses_json import dataclass_json

from flytekit import task
from flytekit.core import context_manager
from flytekit.core.context_manager import CompilationState
from flytekit.core.promise import VoidPromise, create_and_link_node, translate_inputs_to_literals
from flytekit.exceptions.user import FlyteAssertion


def test_create_and_link_node():
    @task
    def t1(a: typing.Union[int, typing.List[int]]) -> typing.Union[int, typing.List[int]]:
        return a

    with pytest.raises(FlyteAssertion, match="Cannot create node when not compiling..."):
        ctx = context_manager.FlyteContext.current_context()
        create_and_link_node(ctx, t1, a=3)

    ctx = context_manager.FlyteContext.current_context().with_compilation_state(CompilationState(prefix=""))
    p = create_and_link_node(ctx, t1, a=3)
    assert p.ref.node_id == "n0"
    assert p.ref.var == "o0"
    assert len(p.ref.node.bindings) == 1

    @task
    def t2(a: typing.Optional[int] = None) -> typing.Union[int]:
        return a

    p = create_and_link_node(ctx, t2)
    assert p.ref.var == "o0"
    assert len(p.ref.node.bindings) == 0


@pytest.mark.parametrize(
    "input",
    [2.0, {"i": 1, "a": ["h", "e"]}, [1, 2, 3]],
)
def test_translate_inputs_to_literals(input):
    @dataclass_json
    @dataclass
    class MyDataclass(object):
        i: int
        a: typing.List[str]

    @task
    def t1(a: typing.Union[float, typing.List[int], MyDataclass]):
        print(a)

    ctx = context_manager.FlyteContext.current_context()
    translate_inputs_to_literals(ctx, {"a": input}, t1.interface.inputs, t1.python_interface.inputs)


def test_translate_inputs_to_literals_with_wrong_types():
    ctx = context_manager.FlyteContext.current_context()
    with pytest.raises(TypeError, match="Not a map type union_type"):

        @task
        def t1(a: typing.Union[float, typing.List[int]]):
            print(a)

        translate_inputs_to_literals(ctx, {"a": {"a": 3}}, t1.interface.inputs, t1.python_interface.inputs)

    with pytest.raises(TypeError, match="Not a collection type union_type"):

        @task
        def t1(a: typing.Union[float, typing.Dict[str, int]]):
            print(a)

        translate_inputs_to_literals(ctx, {"a": [1, 2, 3]}, t1.interface.inputs, t1.python_interface.inputs)

    with pytest.raises(
        AssertionError, match="Outputs of a non-output producing task n0 cannot be passed to another task"
    ):

        @task
        def t1(a: typing.Union[float, typing.Dict[str, int]]):
            print(a)

        translate_inputs_to_literals(ctx, {"a": VoidPromise("n0")}, t1.interface.inputs, t1.python_interface.inputs)
