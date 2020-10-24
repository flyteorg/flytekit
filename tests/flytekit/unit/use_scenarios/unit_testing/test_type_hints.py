import datetime
import inspect
import os
import typing

import pandas
import pytest

import flytekit.annotated.task
import flytekit.annotated.workflow
from flytekit.annotated import context_manager, promise
from flytekit.annotated.condition import conditional
from flytekit.annotated.context_manager import ExecutionState
from flytekit.annotated.interface import extract_return_annotation, transform_variable_map
from flytekit.annotated.promise import Promise
from flytekit.annotated.task import AbstractSQLPythonTask, dynamic, maptask, metadata, task
from flytekit.annotated.type_engine import TypeEngine
from flytekit.annotated.workflow import workflow
from flytekit.common.nodes import SdkNode
from flytekit.common.promise import NodeOutput
from flytekit.interfaces.data.data_proxy import FileAccessProvider
from flytekit.models.core import types as _core_types
from flytekit.models.types import LiteralType, SimpleType


def test_default_wf_params_works():
    @task
    def my_task(a: int):
        wf_params = flytekit.current_context()
        assert wf_params.execution_id == "ex:local:local:local"

    my_task(a=3)


def test_simple_input_output():
    @task
    def my_task(a: int) -> typing.NamedTuple("OutputsBC", b=int, c=str):
        ctx = flytekit.current_context()
        assert ctx.execution_id == "ex:local:local:local"
        return a + 2, "hello world"

    assert my_task(a=3) == (5, "hello world")


def test_simple_input_no_output():
    @task
    def my_task(a: int):
        pass

    assert my_task(a=3) is None

    ctx = context_manager.FlyteContext.current_context()
    with ctx.new_compilation_context() as ctx:
        outputs = my_task(a=3)
        assert outputs is None


def test_single_output():
    @task
    def my_task() -> str:
        return "Hello world"

    assert my_task() == "Hello world"

    ctx = context_manager.FlyteContext.current_context()
    with ctx.new_compilation_context() as ctx:
        outputs = my_task()
        assert ctx.compilation_state is not None
        nodes = ctx.compilation_state.nodes
        assert len(nodes) == 1
        assert outputs.is_ready is False
        assert outputs.ref.sdk_node is nodes[0]


def test_named_tuples():
    nt1 = typing.NamedTuple("NT1", x_str=str, y_int=int)

    def x(a: int, b: str) -> typing.NamedTuple("NT1", x_str=str, y_int=int):
        return ("hello world", 5)

    def y(a: int, b: str) -> nt1:
        return nt1("hello world", 5)

    result = transform_variable_map(extract_return_annotation(inspect.signature(x).return_annotation))
    assert result["x_str"].type.simple == 3
    assert result["y_int"].type.simple == 1

    result = transform_variable_map(extract_return_annotation(inspect.signature(y).return_annotation))
    assert result["x_str"].type.simple == 3
    assert result["y_int"].type.simple == 1


def test_unnamed_typing_tuple():
    def z(a: int, b: str) -> typing.Tuple[int, str]:
        return 5, "hello world"

    result = transform_variable_map(extract_return_annotation(inspect.signature(z).return_annotation))
    assert result["out_0"].type.simple == 1
    assert result["out_1"].type.simple == 3


def test_regular_tuple():
    def q(a: int, b: str) -> (int, str):
        return 5, "hello world"

    result = transform_variable_map(extract_return_annotation(inspect.signature(q).return_annotation))
    assert result["out_0"].type.simple == 1
    assert result["out_1"].type.simple == 3


def test_single_output_new_decorator():
    def q(a: int, b: str) -> int:
        return a + len(b)

    result = transform_variable_map(extract_return_annotation(inspect.signature(q).return_annotation))
    assert result["out_0"].type.simple == 1


def test_sig_files():
    def q() -> os.PathLike:
        ...

    result = transform_variable_map(extract_return_annotation(inspect.signature(q).return_annotation))
    assert isinstance(result["out_0"].type.blob, _core_types.BlobType)


def test_engine_file_output():
    basic_blob_type = _core_types.BlobType(format="", dimensionality=_core_types.BlobType.BlobDimensionality.SINGLE,)

    fs = FileAccessProvider(local_sandbox_dir="/tmp/flytetesting")
    with context_manager.FlyteContext.current_context().new_file_access_context(file_access_provider=fs) as ctx:
        # Write some text to a file not in that directory above
        test_file_location = "/tmp/sample.txt"
        with open(test_file_location, "w") as fh:
            fh.write("Hello World\n")

        lit = TypeEngine.to_literal(ctx, test_file_location, os.PathLike, LiteralType(blob=basic_blob_type))

        # Since we're using local as remote, we should be able to just read the file from the 'remote' location.
        with open(lit.scalar.blob.uri, "r") as fh:
            assert fh.readline() == "Hello World\n"

        # We should also be able to turn the thing back into regular python native thing.
        redownloaded_local_file_location = TypeEngine.to_python_value(ctx, lit, os.PathLike)
        with open(redownloaded_local_file_location, "r") as fh:
            assert fh.readline() == "Hello World\n"


def test_wf1():
    @task
    def t1(a: int) -> typing.NamedTuple("OutputsBC", t1_int_output=int, c=str):
        return a + 2, "world"

    @task
    def t2(a: str, b: str) -> str:
        return b + a

    @workflow
    def my_wf(a: int, b: str) -> (int, str):
        x, y = t1(a=a)
        d = t2(a=y, b=b)
        return x, d

    assert len(my_wf._nodes) == 2
    assert my_wf._nodes[0].id == "node-0"
    assert my_wf._nodes[1]._upstream_nodes[0] is my_wf._nodes[0]

    assert len(my_wf._output_bindings) == 2
    assert my_wf._output_bindings[0].var == "out_0"
    assert my_wf._output_bindings[0].binding.promise.var == "t1_int_output"


def test_wf1_run():
    @task
    def t1(a: int) -> typing.NamedTuple("OutputsBC", t1_int_output=int, c=str):
        return a + 2, "world"

    @task
    def t2(a: str, b: str) -> str:
        return b + a

    @workflow
    def my_wf(a: int, b: str) -> (int, str):
        x, y = t1(a=a)
        d = t2(a=y, b=b)
        return x, d

    x = my_wf(a=5, b="hello ")
    assert x == (7, "hello world")


def test_wf1_with_overrides():
    @task
    def t1(a: int) -> typing.NamedTuple("OutputsBC", t1_int_output=int, c=str):
        return a + 2, "world"

    @task
    def t2(a: str, b: str) -> str:
        return b + a

    @workflow
    def my_wf(a: int, b: str) -> (int, str):
        x, y = t1(a=a).with_overrides(name="x")
        d = t2(a=y, b=b).with_overrides()
        return x, d

    x = my_wf(a=5, b="hello ")
    assert x == (7, "hello world")


def test_wf1_with_list_of_inputs():
    @task
    def t1(a: int) -> typing.NamedTuple("OutputsBC", t1_int_output=int, c=str):
        return a + 2, "world"

    @task
    def t2(a: typing.List[str]) -> str:
        return " ".join(a)

    @workflow
    def my_wf(a: int, b: str) -> (int, str):
        x, y = t1(a=a)
        d = t2(a=[b, y])
        return x, d

    x = my_wf(a=5, b="hello")
    assert x == (7, "hello world")


def test_promise_return():
    """
    Testing that when a workflow is local executed but a local wf execution context already exists, Promise objects
    are returned wrapping Flyte literals instead of the unpacked dict.
    """

    @task
    def t1(a: int) -> typing.NamedTuple("OutputsBC", t1_int_output=int, c=str):
        a = a + 2
        return a, "world-" + str(a)

    @workflow
    def mimic_sub_wf(a: int) -> (str, str):
        x, y = t1(a=a)
        u, v = t1(a=x)
        return y, v

    ctx = context_manager.FlyteContext.current_context()

    with ctx.new_execution_context(mode=ExecutionState.Mode.LOCAL_WORKFLOW_EXECUTION) as ctx:
        a, b = mimic_sub_wf(a=3)

    assert isinstance(a, promise.Promise)
    assert isinstance(b, promise.Promise)
    assert a.val.scalar.value.string_value == "world-5"
    assert b.val.scalar.value.string_value == "world-7"


def test_wf1_with_subwf():
    @task
    def t1(a: int) -> typing.NamedTuple("OutputsBC", t1_int_output=int, c=str):
        a = a + 2
        return a, "world-" + str(a)

    @task
    def t2(a: str, b: str) -> str:
        return b + a

    @workflow
    def my_subwf(a: int) -> (str, str):
        x, y = t1(a=a)
        u, v = t1(a=x)
        return y, v

    @workflow
    def my_wf(a: int, b: str) -> (int, str, str):
        x, y = t1(a=a).with_overrides()
        u, v = my_subwf(a=x)
        return x, u, v

    x = my_wf(a=5, b="hello ")
    assert x == (7, "world-9", "world-11")


def test_wf1_with_sql():
    sql = AbstractSQLPythonTask(
        "my-query",
        query_template="SELECT * FROM hive.city.fact_airport_sessions WHERE ds = '{{ .Inputs.ds }}' LIMIT 10",
        inputs={"ds": datetime.datetime},
        metadata=metadata(retries=2),
    )

    @task
    def t1() -> datetime.datetime:
        return datetime.datetime.now()

    @workflow
    def my_wf():
        dt = t1()
        sql(ds=dt)

    my_wf()


def test_wf1_with_spark():
    @task(task_type="spark")
    def my_spark(spark_session, a: int) -> typing.NamedTuple("OutputsBC", t1_int_output=int, c=str):
        return a + 2, "world"

    @task
    def t2(a: str, b: str) -> str:
        return b + a

    @workflow
    def my_wf(a: int, b: str) -> (int, str):
        x, y = my_spark(a=a)
        d = t2(a=y, b=b)
        return x, d

    x = my_wf(a=5, b="hello ")
    assert x == (7, "hello world")


def test_wf1_with_map():
    @task
    def t1(a: int) -> typing.NamedTuple("OutputsBC", t1_int_output=int, c=str):
        a = a + 2
        return a, "world-" + str(a)

    @task
    def t2(a: typing.List[int], b: typing.List[str]) -> (int, str):
        ra = 0
        for x in a:
            ra += x
        rb = ""
        for x in b:
            rb += x
        return ra, rb

    @workflow
    def my_wf(a: typing.List[int]) -> (int, str):
        x, y = maptask(t1, metadata=metadata(retries=1))(a=a)
        return t2(a=x, b=y)

    x = my_wf(a=[5, 6])
    assert x == (15, "world-7world-8")


def test_wf1_compile_time_constant_vars():
    @task
    def t1(a: int) -> typing.NamedTuple("OutputsBC", t1_int_output=int, c=str):
        return a + 2, "world"

    @task
    def t2(a: str, b: str) -> str:
        return b + a

    @workflow
    def my_wf(a: int, b: str) -> (int, str):
        x, y = t1(a=a)
        d = t2(a="This is my way", b=b)
        return x, d

    x = my_wf(a=5, b="hello ")
    assert x == (7, "hello This is my way")


def test_wf1_with_dynamic():
    @task
    def t1(a: int) -> str:
        a = a + 2
        return "world-" + str(a)

    @task
    def t2(a: str, b: str) -> str:
        return b + a

    @dynamic
    def my_subwf(a: int) -> typing.List[str]:
        s = []
        for i in range(a):
            s.append(t1(a=i))
        return s

    @workflow
    def my_wf(a: int, b: str) -> (str, typing.List[str]):
        x = t2(a=b, b=b)
        v = my_subwf(a=a)
        return x, v

    v = 5
    x = my_wf(a=v, b="hello ")
    assert x == ("hello hello ", ["world-" + str(i) for i in range(2, v + 2)])

    compiled_sub_wf = my_subwf.compile_into_workflow(a=5)
    assert len(compiled_sub_wf._nodes) == 5


def test_list_output():
    @task
    def t1(a: int) -> str:
        a = a + 2
        return "world-" + str(a)

    @workflow
    def lister() -> typing.List[str]:
        s = []
        # FYI: For users who happen to look at this, keep in mind this is only run once at compile time.
        for i in range(10):
            s.append(t1(a=i))
        return s

    assert len(lister.interface.outputs) == 1
    binding_data = lister._output_bindings[0].binding  # the property should be named binding_data
    assert binding_data.collection is not None
    assert len(binding_data.collection.bindings) == 10


def test_comparison_refs():
    def dummy_node(id) -> SdkNode:
        n = SdkNode(id, [], None, None, sdk_task=AbstractSQLPythonTask("x", "x", [], metadata()))
        n._id = id
        return n

    px = Promise("x", NodeOutput(var="x", sdk_type=LiteralType(simple=SimpleType.INTEGER), sdk_node=dummy_node("n1")))
    py = Promise("y", NodeOutput(var="y", sdk_type=LiteralType(simple=SimpleType.INTEGER), sdk_node=dummy_node("n2")))

    def print_expr(expr):
        print(f"{expr} is type {type(expr)}")

    print_expr(px == py)
    print_expr(px < py)
    print_expr((px == py) & (px < py))
    print_expr(((px == py) & (px < py)) | (px > py))
    print_expr(px < 5)
    print_expr(px >= 5)


def test_comparison_lits():
    px = Promise("x", TypeEngine.to_literal(None, 5, int, None))
    py = Promise("y", TypeEngine.to_literal(None, 8, int, None))

    def eval_expr(expr, expected: bool):
        print(f"{expr} evals to {expr.eval()}")
        assert expected == expr.eval()

    eval_expr(px == py, False)
    eval_expr(px < py, True)
    eval_expr((px == py) & (px < py), False)
    eval_expr(((px == py) & (px < py)) | (px > py), False)
    eval_expr(px < 5, False)
    eval_expr(px >= 5, True)
    eval_expr(py >= 5, True)


def test_wf1_branches():
    @task
    def t1(a: int) -> typing.NamedTuple("OutputsBC", t1_int_output=int, c=str):
        return a + 2, "world"

    @task
    def t2(a: str) -> str:
        return a

    @workflow
    def my_wf(a: int, b: str) -> (int, str):
        x, y = t1(a=a)
        print(x)
        d = conditional().if_(x == 4).then(t2(a=b)).elif_(x >= 5).then(t2(a=y)).else_().fail("Unable to choose branch")
        return x, d

    x = my_wf(a=5, b="hello ")
    assert x == (7, "world")


def test_wf1_branches_no_else():
    with pytest.raises(AssertionError):

        def foo():
            @task
            def t1(a: int) -> typing.NamedTuple("OutputsBC", t1_int_output=int, c=str):
                return a + 2, "world"

            @task
            def t2(a: str) -> str:
                return a

            @workflow
            def my_wf(a: int, b: str) -> (int, str):
                x, y = t1(a=a)
                print(x)
                d = conditional().if_(x == 4).then(t2(a=b)).elif_(x >= 5).then(t2(a=y))
                return x, d

            @workflow
            def my_wf2(a: int, b: str) -> (int, str):
                x, y = t1(a=a)
                print(x)
                d = (
                    conditional()
                    .if_(x == 4)
                    .then(t2(a=b))
                    .elif_(x >= 5)
                    .then(t2(a=y))
                    .else_()
                    .then(t2(a="Ok I give up!"))
                )
                return x, d

        foo()


def test_wf1_branches_failing():
    @task
    def t1(a: int) -> typing.NamedTuple("OutputsBC", t1_int_output=int, c=str):
        return a + 2, "world"

    @task
    def t2(a: str) -> str:
        return a

    @workflow
    def my_wf(a: int, b: str) -> (int, str):
        x, y = t1(a=a)
        print(x)
        d = conditional().if_(x == 4).then(t2(a=b)).elif_(x >= 5).then(t2(a=y)).else_().fail("All Branches failed")
        return x, d

    with pytest.raises(AssertionError):
        my_wf(a=1, b="hello ")


def test_wf1_df():
    @task
    def t1(a: int) -> pandas.DataFrame:
        return pandas.DataFrame(data={'col1': [a, 2], 'col2': [a, 4]})

    @task
    def t2(df: pandas.DataFrame) -> pandas.DataFrame:
        return df.append(pandas.DataFrame(data={'col1': [5, 10], 'col2': [5, 10]}))

    @workflow
    def my_wf(a: int) -> pandas.DataFrame:
        df = t1(a=a)
        return t2(df=df)

    x = my_wf(a=20)
    assert isinstance(x, pandas.DataFrame)
    assert x.reset_index(drop=True) == \
           pandas.DataFrame(data={'col1': [20, 2, 5, 10], 'col2': [20, 4, 5, 10]}).reset_index(drop=True)

# TODO Add an example that shows how tuple fails and it should fail cleanly. As tuple types are not supported!
