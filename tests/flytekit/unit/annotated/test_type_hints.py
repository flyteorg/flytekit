import datetime
import os
import typing

import pandas
import pytest

import flytekit
from flytekit import ContainerTask, SQLTask, dynamic, maptask, metadata, task
from flytekit import typing as flytekit_typing
from flytekit import workflow
from flytekit.annotated import context_manager, launch_plan, promise
from flytekit.annotated.condition import conditional
from flytekit.annotated.context_manager import ExecutionState
from flytekit.annotated.promise import Promise
from flytekit.annotated.task import kwtypes
from flytekit.annotated.testing import task_mock
from flytekit.annotated.type_engine import RestrictedTypeError, TypeEngine
from flytekit.common.nodes import SdkNode
from flytekit.common.promise import NodeOutput
from flytekit.interfaces.data.data_proxy import FileAccessProvider
from flytekit.models.core import types as _core_types
from flytekit.models.interface import Parameter
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

    @workflow
    def my_wf2(a: int, b: str) -> int:
        x, y = t1(a=a)
        t2(a=[b, y])
        return x

    x = my_wf2(a=5, b="hello")
    assert x == 7


def test_wf_output_mismatch():
    with pytest.raises(AssertionError):

        @workflow
        def my_wf(a: int, b: str) -> (int, str):
            return a

    with pytest.raises(AssertionError):

        @workflow
        def my_wf2(a: int, b: str) -> int:
            return a, b

    @workflow
    def my_wf3(a: int, b: str) -> int:
        return (a,)

    my_wf3(a=10, b="hello")


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
    sql = SQLTask(
        "my-query",
        query_template="SELECT * FROM hive.city.fact_airport_sessions WHERE ds = '{{ .Inputs.ds }}' LIMIT 10",
        inputs=kwtypes(ds=datetime.datetime),
        metadata=metadata(retries=2),
    )

    @task
    def t1() -> datetime.datetime:
        return datetime.datetime.now()

    @workflow
    def my_wf() -> str:
        dt = t1()
        return sql(ds=dt)

    with task_mock(sql) as mock:
        mock.return_value = "Hello"
        assert my_wf() == "Hello"


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


def test_wf1_with_constant_return():
    @task
    def t1(a: int) -> typing.NamedTuple("OutputsBC", t1_int_output=int, c=str):
        return a + 2, "world"

    @task
    def t2(a: str, b: str) -> str:
        return b + a

    @workflow
    def my_wf(a: int, b: str) -> (int, str):
        x, y = t1(a=a)
        t2(a="This is my way", b=b)
        return x, "A constant output"

    x = my_wf(a=5, b="hello ")
    assert x == (7, "A constant output")

    @workflow
    def my_wf2(a: int, b: str) -> int:
        t1(a=a)
        t2(a="This is my way", b=b)
        return 10

    assert my_wf2(a=5, b="hello ") == 10


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

    with context_manager.FlyteContext.current_context().new_registration_settings(
        registration_settings=context_manager.RegistrationSettings(
            project="test_proj", domain="test_domain", version="abc", image="image:name", env={},
        )
    ) as ctx:
        with ctx.new_execution_context(mode=ExecutionState.Mode.TASK_EXECUTION) as ctx:
            dynamic_job_spec = my_subwf.compile_into_workflow(ctx, a=5)
            assert len(dynamic_job_spec._nodes) == 5


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
        n = SdkNode(id, [], None, None, sdk_task=SQLTask("x", "x", [], metadata()))
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
        d = (
            conditional("test1")
            .if_(x == 4)
            .then(t2(a=b))
            .elif_(x >= 5)
            .then(t2(a=y))
            .else_()
            .fail("Unable to choose branch")
        )
        f = conditional("test2").if_(d == "hello ").then(t2(a="It is hello")).else_().then(t2(a="Not Hello!"))
        return x, f

    x = my_wf(a=5, b="hello ")
    assert x == (7, "Not Hello!")

    x = my_wf(a=2, b="hello ")
    assert x == (4, "It is hello")


def test_wf1_branches_no_else():
    with pytest.raises(NotImplementedError):

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
                d = conditional("test1").if_(x == 4).then(t2(a=b)).elif_(x >= 5).then(t2(a=y))
                conditional("test2").if_(x == 4).then(t2(a=b)).elif_(x >= 5).then(t2(a=y)).else_().fail("blah")
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
        d = (
            conditional("test1")
            .if_(x == 4)
            .then(t2(a=b))
            .elif_(x >= 5)
            .then(t2(a=y))
            .else_()
            .fail("All Branches failed")
        )
        return x, d

    with pytest.raises(ValueError):
        my_wf(a=1, b="hello ")


def test_cant_use_normal_tuples():
    with pytest.raises(RestrictedTypeError):

        @task
        def t1(a: str) -> tuple:
            return (a, 3)


def test_file_type_in_workflow_with_bad_format():
    @task
    def t1() -> flytekit_typing.FlyteFilePath["txt"]:
        fname = "/tmp/flytekit_test"
        with open(fname, "w") as fh:
            fh.write("Hello World\n")
        return fname

    @workflow
    def my_wf() -> flytekit_typing.FlyteFilePath["txt"]:
        f = t1()
        return f

    res = my_wf()
    with open(res, "r") as fh:
        assert fh.read() == "Hello World\n"


# def test_more_file_handling():
#     SAMPLE_DATA = "https://raw.githubusercontent.com/jbrownlee/Datasets/master/pima-indians-diabetes.data.csv",
#
#     @task
#     def t1(fname: os.PathLike) -> int:
#         # Should not copy the data to s3, should download the link and put into tmp dir every time it's run, either
#         # locally or in production
#         with open(fname, "r") as fh:
#             print(fh.readlines())
#         return 3
#
#     @workflow
#     def my_wf() -> flytekit_typing.FlyteFilePath["txt"]:
#         f = t1()
#         return f


#
#     @task
#     def t12(
#         fname: flytekit_typing.FlyteFilePath[
#             "csv"
#         ] = "/some/local/pima-indians-diabetes.data.csv",
#     ) -> int:
#         # This should copy to s3
#         with open(fname, "r") as fh:
#             ...
#         return 3
#
#     @task
#     def t2() -> flytekit_typing.FlyteFilePath:
#         # I want to download a copy of blah, store it in s3 and return the new path in the literal
#         return "https://raw.github.com/blah"
#
#     @task
#     def t3() -> flytekit_typing.FlyteFilePath:
#         # I don't want to download a copy of blah, I want this path in the literal.
#         return "https://raw.github.com/blah"
#
#     @task
#     def t4() -> flytekit_typing.FlyteFilePath:
#         # Upload this to S3, the downstream task will read it from S3
#         return "/opt/some/new/file"
#
#     @task
#     def t5() -> flytekit_typing.FlyteFilePath:
#         # Don't upload this to s3, the next task that takes this output relies on this exact path.
#         return "/opt/some/file/in/the/container"
#
#     @task
#     def t6(
#         fname: flytekit_typing.FlyteFilePath[
#             "csv"
#         ] = "https://raw.githubusercontent.com/jbrownlee/Datasets/master/pima-indians-diabetes.data.csv",
#     ) -> int:
#         with open(fname, "r") as fh:
#             ...
#         return 3
#


def test_wf1_df():
    @task
    def t1(a: int) -> pandas.DataFrame:
        return pandas.DataFrame(data={"col1": [a, 2], "col2": [a, 4]})

    @task
    def t2(df: pandas.DataFrame) -> pandas.DataFrame:
        return df.append(pandas.DataFrame(data={"col1": [5, 10], "col2": [5, 10]}))

    @workflow
    def my_wf(a: int) -> pandas.DataFrame:
        df = t1(a=a)
        return t2(df=df)

    x = my_wf(a=20)
    assert isinstance(x, pandas.DataFrame)
    result_df = x.reset_index(drop=True) == pandas.DataFrame(
        data={"col1": [20, 2, 5, 10], "col2": [20, 4, 5, 10]}
    ).reset_index(drop=True)
    assert result_df.all().all()


def test_lp_default_handling():
    @task
    def t1(a: int) -> typing.NamedTuple("OutputsBC", t1_int_output=int, c=str):
        a = a + 2
        return a, "world-" + str(a)

    @workflow
    def my_wf(a: int, b: int) -> (str, str, int, int):
        x, y = t1(a=a)
        u, v = t1(a=b)
        return y, v, x, u

    lp = launch_plan.LaunchPlan.create("test1", my_wf)
    assert len(lp.parameters.parameters) == 0
    assert len(lp.fixed_inputs.literals) == 0

    lp_with_defaults = launch_plan.LaunchPlan.create("test2", my_wf, default_inputs={"a": 3})
    assert len(lp_with_defaults.parameters.parameters) == 1
    assert len(lp_with_defaults.fixed_inputs.literals) == 0

    lp_with_fixed = launch_plan.LaunchPlan.create("test3", my_wf, fixed_inputs={"a": 3})
    assert len(lp_with_fixed.parameters.parameters) == 0
    assert len(lp_with_fixed.fixed_inputs.literals) == 1

    @workflow
    def my_wf2(a: int, b: int = 42) -> (str, str, int, int):
        x, y = t1(a=a)
        u, v = t1(a=b)
        return y, v, x, u

    lp = launch_plan.LaunchPlan.create("test4", my_wf2)
    assert len(lp.parameters.parameters) == 1
    assert len(lp.fixed_inputs.literals) == 0

    lp_with_defaults = launch_plan.LaunchPlan.create("test5", my_wf2, default_inputs={"a": 3})
    assert len(lp_with_defaults.parameters.parameters) == 2
    assert len(lp_with_defaults.fixed_inputs.literals) == 0
    # Launch plan defaults override wf defaults
    assert lp_with_defaults(b=3) == ("world-5", "world-5", 5, 5)

    lp_with_fixed = launch_plan.LaunchPlan.create("test6", my_wf2, fixed_inputs={"a": 3})
    assert len(lp_with_fixed.parameters.parameters) == 1
    assert len(lp_with_fixed.fixed_inputs.literals) == 1
    # Launch plan defaults override wf defaults
    assert lp_with_fixed(b=3) == ("world-5", "world-5", 5, 5)

    lp_with_fixed = launch_plan.LaunchPlan.create("test7", my_wf2, fixed_inputs={"b": 3})
    assert len(lp_with_fixed.parameters.parameters) == 0
    assert len(lp_with_fixed.fixed_inputs.literals) == 1


def test_wf1_with_lp_node():
    @task
    def t1(a: int) -> typing.NamedTuple("OutputsBC", t1_int_output=int, c=str):
        a = a + 2
        return a, "world-" + str(a)

    @workflow
    def my_subwf(a: int) -> (str, str):
        x, y = t1(a=a)
        u, v = t1(a=x)
        return y, v

    lp = launch_plan.LaunchPlan.create("lp_nodetest1", my_subwf)
    lp_with_defaults = launch_plan.LaunchPlan.create("lp_nodetest2", my_subwf, default_inputs={"a": 3})

    @workflow
    def my_wf(a: int = 42) -> (int, str, str):
        x, y = t1(a=a).with_overrides()
        u, v = lp(a=x)
        return x, u, v

    x = my_wf(a=5)
    assert x == (7, "world-9", "world-11")

    assert my_wf() == (44, "world-46", "world-48")

    @workflow
    def my_wf2(a: int = 42) -> (int, str, str, str):
        x, y = t1(a=a).with_overrides()
        u, v = lp_with_defaults()
        return x, y, u, v

    assert my_wf2() == (44, "world-44", "world-5", "world-7")

    @workflow
    def my_wf3(a: int = 42) -> (int, str, str, str):
        x, y = t1(a=a).with_overrides()
        u, v = lp_with_defaults(a=x)
        return x, y, u, v

    assert my_wf2() == (44, "world-44", "world-5", "world-7")


def test_lp_serialize():
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

    lp = launch_plan.LaunchPlan.create("serialize_test1", my_subwf)
    lp_with_defaults = launch_plan.LaunchPlan.create("serialize_test2", my_subwf, default_inputs={"a": 3})

    registration_settings = context_manager.RegistrationSettings(
        project="proj",
        domain="dom",
        version="123",
        image="asdf/fdsa:123",
        env={},
        iam_role="test:iam:role",
        service_account=None,
    )
    with context_manager.FlyteContext.current_context().new_registration_settings(
        registration_settings=registration_settings
    ):
        sdk_lp = lp.get_registerable_entity()
        assert len(sdk_lp.default_inputs.parameters) == 0
        assert len(sdk_lp.fixed_inputs.literals) == 0

        sdk_lp = lp_with_defaults.get_registerable_entity()
        assert len(sdk_lp.default_inputs.parameters) == 1
        assert len(sdk_lp.fixed_inputs.literals) == 0

        # Adding a check to make sure oneof is respected. Tricky with booleans... if a default is specified, the
        # required field needs to be None, not False.
        parameter_a = sdk_lp.default_inputs.parameters["a"]
        parameter_a = Parameter.from_flyte_idl(parameter_a.to_flyte_idl())
        assert parameter_a.default is not None


def test_wf_container_task():
    @task
    def t1(a: int) -> (int, str):
        return a + 2, str(a) + "-HELLO"

    t2 = ContainerTask(
        "raw",
        image="alpine",
        inputs=kwtypes(a=int, b=str),
        input_data_dir="/tmp",
        output_data_dir="/tmp",
        command=["cat"],
        arguments=["/tmp/a"],
        metadata=metadata(),
    )

    def wf(a: int):
        x, y = t1(a=a)
        t2(a=x, b=y)

    with task_mock(t2) as mock:
        mock.side_effect = lambda a, b: None
        assert t2(a=10, b="hello") is None

        wf(a=10)


def test_wf_container_task_multiple():
    square = ContainerTask(
        name="square",
        metadata=metadata(),
        input_data_dir="/var/inputs",
        output_data_dir="/var/outputs",
        inputs=kwtypes(val=int),
        outputs=kwtypes(out=int),
        image="alpine",
        command=["sh", "-c", "echo $(( {{.Inputs.val}} * {{.Inputs.val}} )) | tee /var/outputs/out"],
    )

    sum = ContainerTask(
        name="sum",
        metadata=metadata(),
        input_data_dir="/var/flyte/inputs",
        output_data_dir="/var/flyte/outputs",
        inputs=kwtypes(x=int, y=int),
        outputs=kwtypes(out=int),
        image="alpine",
        command=["sh", "-c", "echo $(( {{.Inputs.x}} + {{.Inputs.y}} )) | tee /var/flyte/outputs/out"],
    )

    @workflow
    def raw_container_wf(val1: int, val2: int) -> int:
        return sum(x=square(val=val1), y=square(val=val2))

    with task_mock(square) as square_mock, task_mock(sum) as sum_mock:
        square_mock.side_effect = lambda val: val * val
        assert square(val=10) == 100

        sum_mock.side_effect = lambda x, y: x + y
        assert sum(x=10, y=10) == 20

        assert raw_container_wf(val1=10, val2=10) == 200


def test_wf_tuple_fails():
    with pytest.raises(RestrictedTypeError):

        @task
        def t1(a: tuple) -> (int, str):
            return a[0] + 2, str(a) + "-HELLO"
