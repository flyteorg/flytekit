import typing
from dataclasses import dataclass

import pytest
from dataclasses_json import dataclass_json
from typing_extensions import Annotated

from flytekit import LaunchPlan, task, workflow
from flytekit.core import context_manager
from flytekit.core.context_manager import CompilationState
from flytekit.core.promise import (
    VoidPromise,
    create_and_link_node,
    create_and_link_node_from_remote,
    translate_inputs_to_literals,
)
from flytekit.exceptions.user import FlyteAssertion
from flytekit.types.pickle import FlytePickle
from flytekit.types.pickle.pickle import BatchSize


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
    def t2(a: typing.Optional[int] = None) -> typing.Optional[int]:
        return a

    p = create_and_link_node(ctx, t2)
    assert p.ref.var == "o0"
    assert len(p.ref.node.bindings) == 0


def test_create_and_link_node_from_remote():
    @task
    def t1() -> None:
        ...

    with pytest.raises(FlyteAssertion, match="Cannot create node when not compiling..."):
        ctx = context_manager.FlyteContext.current_context()
        create_and_link_node_from_remote(ctx, t1, a=3)

    ctx = context_manager.FlyteContext.current_context().with_compilation_state(CompilationState(prefix=""))
    p = create_and_link_node_from_remote(ctx, t1)
    assert p.ref.node_id == "n0"
    assert p.ref.var == "placeholder"
    assert len(p.ref.node.bindings) == 0

    @task
    def t2(a: int) -> int:
        return a

    p = create_and_link_node_from_remote(ctx, t2, a=3)
    assert p.ref.var == "o0"
    assert len(p.ref.node.bindings) == 1


def test_create_and_link_node_from_remote_ignore():
    @workflow
    def wf(i: int, j: int):
        ...

    lp = LaunchPlan.get_or_create(wf, name="promise-test", fixed_inputs={"i": 1}, default_inputs={"j": 10})
    ctx = context_manager.FlyteContext.current_context().with_compilation_state(CompilationState(prefix=""))

    # without providing the _inputs_not_allowed or _ignorable_inputs, all inputs to lp become required,
    # which is incorrect
    with pytest.raises(FlyteAssertion, match="Missing input `i` type `<FlyteLiteral simple: INTEGER>`"):
        create_and_link_node_from_remote(ctx, lp)

    # Even if j is not provided it will default
    create_and_link_node_from_remote(ctx, lp, _inputs_not_allowed={"i"}, _ignorable_inputs={"j"})

    # value of `i` cannot be overriden
    with pytest.raises(
        FlyteAssertion, match="ixed inputs cannot be specified. Please remove the following inputs - {'i'}"
    ):
        create_and_link_node_from_remote(ctx, lp, _inputs_not_allowed={"i"}, _ignorable_inputs={"j"}, i=15)

    # It is ok to override `j` which is a default input
    create_and_link_node_from_remote(ctx, lp, _inputs_not_allowed={"i"}, _ignorable_inputs={"j"}, j=15)


@pytest.mark.parametrize(
    "input",
    [2.0, {"i": 1, "a": ["h", "e"]}, [1, 2, 3], ["foo"] * 5],
)
def test_translate_inputs_to_literals(input):
    @dataclass_json
    @dataclass
    class MyDataclass(object):
        i: int
        a: typing.List[str]

    @task
    def t1(a: typing.Union[float, typing.List[int], MyDataclass, Annotated[typing.List[FlytePickle], BatchSize(2)]]):
        print(a)

    ctx = context_manager.FlyteContext.current_context()
    translate_inputs_to_literals(ctx, {"a": input}, t1.interface.inputs, t1.python_interface.inputs)


def test_translate_inputs_to_literals_with_wrong_types():
    ctx = context_manager.FlyteContext.current_context()
    with pytest.raises(TypeError, match="Not a map type <FlyteLiteral union_type"):

        @task
        def t1(a: typing.Union[float, typing.List[int]]):
            print(a)

        translate_inputs_to_literals(ctx, {"a": {"a": 3}}, t1.interface.inputs, t1.python_interface.inputs)

    with pytest.raises(TypeError, match="Not a collection type <FlyteLiteral union_type"):

        @task
        def t1(a: typing.Union[float, typing.Dict[str, int]]):
            print(a)

        translate_inputs_to_literals(ctx, {"a": [1, 2, 3]}, t1.interface.inputs, t1.python_interface.inputs)

    with pytest.raises(
        AssertionError,
        match="Outputs of a non-output producing task n0 cannot be passed to another task",
    ):

        @task
        def t1(a: typing.Union[float, typing.Dict[str, int]]):
            print(a)

        translate_inputs_to_literals(
            ctx,
            {"a": VoidPromise("n0")},
            t1.interface.inputs,
            t1.python_interface.inputs,
        )
