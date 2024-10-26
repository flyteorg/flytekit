import sys
import typing
from dataclasses import dataclass
from typing import Dict, List

import pytest
from dataclasses_json import DataClassJsonMixin, dataclass_json
from typing_extensions import Annotated

from flytekit import LaunchPlan, task, workflow
from flytekit.core import context_manager
from flytekit.core.context_manager import CompilationState, FlyteContextManager
from flytekit.core.promise import (
    Promise,
    VoidPromise,
    binding_data_from_python_std,
    create_and_link_node,
    create_and_link_node_from_remote,
    resolve_attr_path_in_promise,
    translate_inputs_to_literals,
)
from flytekit.core.type_engine import TypeEngine
from flytekit.exceptions.user import FlyteAssertion, FlytePromiseAttributeResolveException
from flytekit.models import literals as literal_models
from flytekit.models.types import LiteralType, SimpleType, TypeStructure


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
    assert len(ctx.compilation_state.nodes) == 1

    @task
    def t2(a: typing.Optional[int] = None) -> typing.Optional[int]:
        return a

    ctx = context_manager.FlyteContext.current_context().with_compilation_state(CompilationState(prefix=""))
    p = create_and_link_node(ctx, t2)
    assert p.ref.var == "o0"
    assert len(p.ref.node.bindings) == 1
    assert len(ctx.compilation_state.nodes) == 1

    ctx = context_manager.FlyteContext.current_context().with_compilation_state(CompilationState(prefix=""))
    p = create_and_link_node(ctx, t2, add_node_to_compilation_state=False)
    assert p.ref.var == "o0"
    assert len(p.ref.node.bindings) == 1
    assert len(ctx.compilation_state.nodes) == 0


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

    ctx = context_manager.FlyteContext.current_context().with_compilation_state(CompilationState(prefix=""))
    p = create_and_link_node_from_remote(ctx, t2, a=3)
    assert p.ref.var == "o0"
    assert len(p.ref.node.bindings) == 1
    assert len(ctx.compilation_state.nodes) == 1

    ctx = context_manager.FlyteContext.current_context().with_compilation_state(CompilationState(prefix=""))
    p = create_and_link_node_from_remote(ctx, t2, add_node_to_compilation_state=False, a=3)
    assert p.ref.var == "o0"
    assert len(p.ref.node.bindings) == 1
    assert len(ctx.compilation_state.nodes) == 0


def test_create_and_link_node_from_remote_ignore():
    @workflow
    def wf(i: int, j: int):
        ...

    lp = LaunchPlan.get_or_create(wf, name="promise-test", fixed_inputs={"i": 1}, default_inputs={"j": 10})
    lp_without_fixed_inpus = LaunchPlan.get_or_create(
        wf, name="promise-test-no-fixed", fixed_inputs=None, default_inputs={"j": 10}
    )
    ctx = context_manager.FlyteContext.current_context().with_compilation_state(CompilationState(prefix=""))

    # without providing the _inputs_not_allowed or _ignorable_inputs, all inputs to lp become required,
    # which is incorrect
    with pytest.raises(
        FlyteAssertion,
        match=r"Missing input `i` type `Flyte Serialized object \(LiteralType\):",
    ):
        create_and_link_node_from_remote(ctx, lp)

    # Even if j is not provided, it will default
    create_and_link_node_from_remote(ctx, lp, _inputs_not_allowed={"i"}, _ignorable_inputs={"j"})

    # Even if i,j is not provided, it will default
    create_and_link_node_from_remote(
        ctx, lp_without_fixed_inpus, _inputs_not_allowed=None, _ignorable_inputs={"i", "j"}
    )

    # value of `i` cannot be overridden
    with pytest.raises(
        FlyteAssertion, match="Fixed inputs cannot be specified. Please remove the following inputs - {'i'}"
    ):
        create_and_link_node_from_remote(ctx, lp, _inputs_not_allowed={"i"}, _ignorable_inputs={"j"}, i=15)

    # It is ok to override `j` which is a default input
    create_and_link_node_from_remote(ctx, lp, _inputs_not_allowed={"i"}, _ignorable_inputs={"j"}, j=15)


@dataclass
class MyDataclass(DataClassJsonMixin):
    i: int
    a: typing.List[str]


@pytest.mark.skipif("pandas" not in sys.modules, reason="Pandas is not installed.")
@pytest.mark.parametrize(
    "input",
    [2.0, MyDataclass(i=1, a=["h", "e"]), [1, 2, 3], ["foo"] * 5],
)
def test_translate_inputs_to_literals(input):
    @task
    def t1(a: typing.Union[float, MyDataclass, typing.List[typing.Any]]):
        print(a)

    ctx = context_manager.FlyteContext.current_context()
    translate_inputs_to_literals(ctx, {"a": input}, t1.interface.inputs, t1.python_interface.inputs)


def test_translate_inputs_to_literals_with_list():
    ctx = context_manager.FlyteContext.current_context()

    @dataclass_json
    @dataclass
    class Foo:
        b: str

    @dataclass_json
    @dataclass
    class Bar:
        a: List[Foo]
        b: str

    src = {"a": [Bar(a=[Foo(b="foo")], b="bar")]}
    src_lit = TypeEngine.to_literal(
        ctx,
        src,
        Dict[str, List[Bar]],
        TypeEngine.to_literal_type(Dict[str, List[Bar]]),
    )
    src_promise = Promise("val1", src_lit)

    i0 = "foo"
    i1 = Promise("n1", TypeEngine.to_literal(ctx, "bar", str, TypeEngine.to_literal_type(str)))

    @task
    def t1(a: List[str]):
        print(a)


    lits = translate_inputs_to_literals(ctx, {"a": [
        i0, i1, src_promise["a"][0].a[0]["b"], src_promise["a"][0]["b"]
    ]}, t1.interface.inputs, t1.python_interface.inputs)
    literal_map = literal_models.LiteralMap(literals=lits)
    python_values = TypeEngine.literal_map_to_kwargs(ctx, literal_map, t1.python_interface.inputs)["a"]
    assert python_values == ["foo", "bar", "foo", "bar"]


def test_translate_inputs_to_literals_with_dict():
    ctx = context_manager.FlyteContext.current_context()

    @dataclass_json
    @dataclass
    class Foo:
        b: str

    @dataclass_json
    @dataclass
    class Bar:
        a: List[Foo]
        b: str

    src = {"a": [Bar(a=[Foo(b="foo")], b="bar")]}
    src_lit = TypeEngine.to_literal(
        ctx,
        src,
        Dict[str, List[Bar]],
        TypeEngine.to_literal_type(Dict[str, List[Bar]]),
    )
    src_promise = Promise("val1", src_lit)

    i0 = "foo"
    i1 = Promise("n1", TypeEngine.to_literal(ctx, "bar", str, TypeEngine.to_literal_type(str)))

    @task
    def t1(a: Dict[str, str]):
        print(a)


    lits = translate_inputs_to_literals(ctx, {"a": {
        "k0": i0,
        "k1": i1,
        "k2": src_promise["a"][0].a[0]["b"],
        "k3": src_promise["a"][0]["b"]
    }}, t1.interface.inputs, t1.python_interface.inputs)
    literal_map = literal_models.LiteralMap(literals=lits)
    python_values = TypeEngine.literal_map_to_kwargs(ctx, literal_map, t1.python_interface.inputs)["a"]
    assert python_values == {
        "k0": "foo",
        "k1": "bar",
        "k2": "foo",
        "k3": "bar",
    }


def test_translate_inputs_to_literals_with_nested_list_and_dict():
    ctx = context_manager.FlyteContext.current_context()

    @dataclass_json
    @dataclass
    class Foo:
        b: str

    @dataclass_json
    @dataclass
    class Bar:
        a: List[Foo]
        b: str

    src = {"a": [Bar(a=[Foo(b="foo")], b="bar")]}
    src_lit = TypeEngine.to_literal(
        ctx,
        src,
        Dict[str, List[Bar]],
        TypeEngine.to_literal_type(Dict[str, List[Bar]]),
    )
    src_promise = Promise("val1", src_lit)

    i0 = "foo"
    i1 = Promise("n1", TypeEngine.to_literal(ctx, "bar", str, TypeEngine.to_literal_type(str)))

    @task
    def t1(a: Dict[str, List[Dict[str, str]]]):
        print(a)

    lits = translate_inputs_to_literals(ctx, {"a": {
        "k0": [{"k00": i0, "k01": i1}],
        "k1": [{"k10": src_promise["a"][0].a[0]["b"], "k11": src_promise["a"][0]["b"]}]
    }}, t1.interface.inputs, t1.python_interface.inputs)
    literal_map = literal_models.LiteralMap(literals=lits)
    python_values = TypeEngine.literal_map_to_kwargs(ctx, literal_map, t1.python_interface.inputs)["a"]
    assert python_values == {
        "k0": [{"k00": "foo", "k01": "bar"}],
        "k1": [{"k10": "foo", "k11": "bar"}]
    }


def test_translate_inputs_to_literals_with_wrong_types():
    ctx = context_manager.FlyteContext.current_context()
    with pytest.raises(TypeError, match="Cannot convert"):

        @task
        def t1(a: typing.Union[float, typing.List[int]]):
            print(a)

        translate_inputs_to_literals(ctx, {"a": {"a": 3}}, t1.interface.inputs, t1.python_interface.inputs)

    with pytest.raises(TypeError, match="Cannot convert"):

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


def test_optional_task_kwargs():
    from typing import Optional

    from flytekit import Workflow

    @task
    def func(foo: Optional[int] = None):
        pass

    wf = Workflow(name="test")
    wf.add_entity(func, foo=None)

    wf()


@pytest.mark.skipif("pandas" not in sys.modules, reason="Pandas is not installed.")
def test_promise_with_attr_path():
    from dataclasses import dataclass
    from typing import Dict, List

    from dataclasses_json import dataclass_json

    @dataclass_json
    @dataclass
    class Foo:
        a: str

    @task
    def t1() -> (List[str], Dict[str, str], Foo):
        return ["a", "b"], {"a": "b"}, Foo(a="b")

    @task
    def t2(a: str) -> str:
        return a

    @workflow
    def my_workflow() -> (str, str, str):
        l, d, f = t1()
        o1 = t2(a=l[0])
        o2 = t2(a=d["a"])
        o3 = t2(a=f.a)
        return o1, o2, o3

    # Run a local execution with promises having attribute path
    o1, o2, o3 = my_workflow()
    assert o1 == "a"
    assert o2 == "b"
    assert o3 == "b"


@pytest.mark.asyncio
@pytest.mark.skipif("pandas" not in sys.modules, reason="Pandas is not installed.")
async def test_resolve_attr_path_in_promise():
    @dataclass_json
    @dataclass
    class Foo:
        b: str

    src = {"a": [Foo(b="foo")]}

    src_lit = TypeEngine.to_literal(
        FlyteContextManager.current_context(),
        src,
        Dict[str, List[Foo]],
        TypeEngine.to_literal_type(Dict[str, List[Foo]]),
    )
    src_promise = Promise("val1", src_lit)

    # happy path
    tgt_promise = await resolve_attr_path_in_promise(src_promise["a"][0]["b"])
    assert "foo" == TypeEngine.to_python_value(FlyteContextManager.current_context(), tgt_promise.val, str)

    # exception
    with pytest.raises(FlytePromiseAttributeResolveException):
        await resolve_attr_path_in_promise(src_promise["c"])


@pytest.mark.asyncio
async def test_prom_with_union_literals():
    ctx = FlyteContextManager.current_context()
    pt = typing.Union[str, int]
    lt = TypeEngine.to_literal_type(pt)
    assert lt.union_type.variants == [
        LiteralType(simple=SimpleType.STRING, structure=TypeStructure(tag="str")),
        LiteralType(simple=SimpleType.INTEGER, structure=TypeStructure(tag="int")),
    ]

    bd = await binding_data_from_python_std(ctx, lt, 3, pt, [])
    assert bd.scalar.union.stored_type.structure.tag == "int"
    bd = await binding_data_from_python_std(ctx, lt, "hello", pt, [])
    assert bd.scalar.union.stored_type.structure.tag == "str"
