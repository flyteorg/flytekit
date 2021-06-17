import datetime
import functools
import os
import typing
from collections import OrderedDict
from dataclasses import dataclass

import pandas
import pytest
from dataclasses_json import dataclass_json

import flytekit
from flytekit import ContainerTask, Secret, SQLTask, dynamic, kwtypes, map_task
from flytekit.common.translator import get_serializable
from flytekit.core import context_manager, launch_plan, promise
from flytekit.core.condition import conditional
from flytekit.core.context_manager import ExecutionState, Image, ImageConfig
from flytekit.core.node import Node
from flytekit.core.promise import NodeOutput, Promise, VoidPromise
from flytekit.core.resources import Resources
from flytekit.core.task import TaskMetadata, task
from flytekit.core.testing import patch, task_mock
from flytekit.core.type_engine import RestrictedTypeError, TypeEngine
from flytekit.core.workflow import workflow
from flytekit.interfaces.data.data_proxy import FileAccessProvider
from flytekit.models import literals as _literal_models
from flytekit.models.core import types as _core_types
from flytekit.models.interface import Parameter
from flytekit.models.task import Resources as _resource_models
from flytekit.models.types import LiteralType
from flytekit.types.schema import FlyteSchema, SchemaOpenMode


def test_default_wf_params_works():
    @task
    def my_task(a: int):
        wf_params = flytekit.current_context()
        assert wf_params.execution_id == "ex:local:local:local"

    my_task(a=3)
    assert context_manager.FlyteContextManager.size() == 1


def test_simple_input_output():
    @task
    def my_task(a: int) -> typing.NamedTuple("OutputsBC", b=int, c=str):
        ctx = flytekit.current_context()
        assert ctx.execution_id == "ex:local:local:local"
        return a + 2, "hello world"

    assert my_task(a=3) == (5, "hello world")
    assert context_manager.FlyteContextManager.size() == 1


def test_simple_input_no_output():
    @task
    def my_task(a: int):
        pass

    assert my_task(a=3) is None

    ctx = context_manager.FlyteContextManager.current_context()
    with context_manager.FlyteContextManager.with_context(ctx.with_new_compilation_state()) as ctx:
        outputs = my_task(a=3)
        assert isinstance(outputs, VoidPromise)

    assert context_manager.FlyteContextManager.size() == 1


def test_single_output():
    @task
    def my_task() -> str:
        return "Hello world"

    assert my_task() == "Hello world"

    ctx = context_manager.FlyteContextManager.current_context()
    with context_manager.FlyteContextManager.with_context(ctx.with_new_compilation_state()) as ctx:
        outputs = my_task()
        assert ctx.compilation_state is not None
        nodes = ctx.compilation_state.nodes
        assert len(nodes) == 1
        assert outputs.is_ready is False
        assert outputs.ref.node is nodes[0]

    assert context_manager.FlyteContextManager.size() == 1


def test_engine_file_output():
    basic_blob_type = _core_types.BlobType(
        format="",
        dimensionality=_core_types.BlobType.BlobDimensionality.SINGLE,
    )

    fs = FileAccessProvider(local_sandbox_dir="/tmp/flytetesting")
    ctx = context_manager.FlyteContextManager.current_context()

    with context_manager.FlyteContextManager.with_context(ctx.with_file_access(fs)) as ctx:
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

    assert context_manager.FlyteContextManager.size() == 1


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
    assert my_wf._nodes[0].id == "n0"
    assert my_wf._nodes[1]._upstream_nodes[0] is my_wf._nodes[0]

    assert len(my_wf._output_bindings) == 2
    assert my_wf._output_bindings[0].var == "o0"
    assert my_wf._output_bindings[0].binding.promise.var == "t1_int_output"

    nt = typing.NamedTuple("SingleNT", t1_int_output=float)

    @task
    def t3(a: int) -> nt:
        return (a + 2,)

    assert t3.python_interface.output_tuple_name == "SingleNT"
    assert t3.interface.outputs["t1_int_output"] is not None
    assert context_manager.FlyteContextManager.size() == 1


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

    @workflow
    def my_wf2(a: int, b: str) -> (int, str):
        tup = t1(a=a)
        d = t2(a=tup.c, b=b)
        return tup.t1_int_output, d

    x = my_wf2(a=5, b="hello ")
    assert x == (7, "hello world")
    assert context_manager.FlyteContextManager.size() == 1


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
    assert context_manager.FlyteContextManager.size() == 1


def test_wf1_with_list_of_inputs():
    @task
    def t1(a: int) -> typing.NamedTuple("OutputsBC", t1_int_output=int, c=str):
        return a + 2, "world"

    @task
    def t2(a: typing.List[str]) -> str:
        return " ".join(a)

    @workflow
    def my_wf(a: int, b: str) -> (int, str):
        xx, yy = t1(a=a)
        d = t2(a=[b, yy])
        return xx, d

    x = my_wf(a=5, b="hello")
    assert x == (7, "hello world")

    @workflow
    def my_wf2(a: int, b: str) -> int:
        x, y = t1(a=a)
        t2(a=[b, y])
        return x

    x = my_wf2(a=5, b="hello")
    assert x == 7
    assert context_manager.FlyteContextManager.size() == 1


def test_wf_output_mismatch():
    with pytest.raises(AssertionError):

        @workflow
        def my_wf(a: int, b: str) -> (int, str):
            return a

    with pytest.raises(AssertionError):

        @workflow
        def my_wf2(a: int, b: str) -> int:
            return a, b

    with pytest.raises(AssertionError):

        @workflow
        def my_wf3(a: int, b: str) -> int:
            return (a,)

    assert context_manager.FlyteContextManager.size() == 1


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

    ctx = context_manager.FlyteContextManager.current_context()

    with context_manager.FlyteContextManager.with_context(
        ctx.with_execution_state(
            ctx.new_execution_state().with_params(mode=ExecutionState.Mode.LOCAL_WORKFLOW_EXECUTION)
        )
    ) as ctx:
        a, b = mimic_sub_wf(a=3)

    assert isinstance(a, promise.Promise)
    assert isinstance(b, promise.Promise)
    assert a.val.scalar.value.string_value == "world-5"
    assert b.val.scalar.value.string_value == "world-7"
    assert context_manager.FlyteContextManager.size() == 1


def test_wf1_with_sql():
    sql = SQLTask(
        "my-query",
        query_template="SELECT * FROM hive.city.fact_airport_sessions WHERE ds = '{{ .Inputs.ds }}' LIMIT 10",
        inputs=kwtypes(ds=datetime.datetime),
        outputs=kwtypes(results=FlyteSchema),
        metadata=TaskMetadata(retries=2),
    )

    @task
    def t1() -> datetime.datetime:
        return datetime.datetime.now()

    @workflow
    def my_wf() -> FlyteSchema:
        dt = t1()
        return sql(ds=dt)

    with task_mock(sql) as mock:
        mock.return_value = pandas.DataFrame(data={"x": [1, 2], "y": ["3", "4"]})
        assert (my_wf().open().all() == pandas.DataFrame(data={"x": [1, 2], "y": ["3", "4"]})).all().all()
    assert context_manager.FlyteContextManager.size() == 1


def test_wf1_with_sql_with_patch():
    sql = SQLTask(
        "my-query",
        query_template="SELECT * FROM hive.city.fact_airport_sessions WHERE ds = '{{ .Inputs.ds }}' LIMIT 10",
        inputs=kwtypes(ds=datetime.datetime),
        outputs=kwtypes(results=FlyteSchema),
        metadata=TaskMetadata(retries=2),
    )

    @task
    def t1() -> datetime.datetime:
        return datetime.datetime.now()

    @workflow
    def my_wf() -> FlyteSchema:
        dt = t1()
        return sql(ds=dt)

    @patch(sql)
    def test_user_demo_test(mock_sql):
        mock_sql.return_value = pandas.DataFrame(data={"x": [1, 2], "y": ["3", "4"]})
        assert (my_wf().open().all() == pandas.DataFrame(data={"x": [1, 2], "y": ["3", "4"]})).all().all()

    # Have to call because tests inside tests don't run
    test_user_demo_test()
    assert context_manager.FlyteContextManager.size() == 1


def test_wf1_with_map():
    @task
    def t1(a: int) -> int:
        a = a + 2
        return a

    @task
    def t2(x: typing.List[int]) -> int:
        return functools.reduce(lambda a, b: a + b, x)

    @workflow
    def my_wf(a: typing.List[int]) -> int:
        x = map_task(t1, metadata=TaskMetadata(retries=1))(a=a)
        return t2(x=x)

    x = my_wf(a=[5, 6])
    assert x == 15
    assert context_manager.FlyteContextManager.size() == 1


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
    assert context_manager.FlyteContextManager.size() == 1


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
    assert context_manager.FlyteContextManager.size() == 1


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

    with context_manager.FlyteContextManager.with_context(
        context_manager.FlyteContextManager.current_context().with_serialization_settings(
            context_manager.SerializationSettings(
                project="test_proj",
                domain="test_domain",
                version="abc",
                image_config=ImageConfig(Image(name="name", fqn="image", tag="name")),
                env={},
            )
        )
    ) as ctx:
        new_exc_state = ctx.execution_state.with_params(mode=ExecutionState.Mode.TASK_EXECUTION)
        with context_manager.FlyteContextManager.with_context(ctx.with_execution_state(new_exc_state)) as ctx:
            dynamic_job_spec = my_subwf.compile_into_workflow(ctx, False, my_subwf._task_function, a=5)
            assert len(dynamic_job_spec._nodes) == 5
            assert len(dynamic_job_spec.tasks) == 1

    assert context_manager.FlyteContextManager.size() == 1


def test_wf1_with_fast_dynamic():
    @task
    def t1(a: int) -> str:
        a = a + 2
        return "fast-" + str(a)

    @dynamic
    def my_subwf(a: int) -> typing.List[str]:
        s = []
        for i in range(a):
            s.append(t1(a=i))
        return s

    @workflow
    def my_wf(a: int) -> typing.List[str]:
        v = my_subwf(a=a)
        return v

    with context_manager.FlyteContextManager.with_context(
        context_manager.FlyteContextManager.current_context().with_serialization_settings(
            context_manager.SerializationSettings(
                project="test_proj",
                domain="test_domain",
                version="abc",
                image_config=ImageConfig(Image(name="name", fqn="image", tag="name")),
                env={},
            )
        )
    ) as ctx:
        with context_manager.FlyteContextManager.with_context(
            ctx.with_execution_state(
                ctx.execution_state.with_params(
                    mode=ExecutionState.Mode.TASK_EXECUTION,
                    additional_context={
                        "dynamic_addl_distro": "s3://my-s3-bucket/fast/123",
                        "dynamic_dest_dir": "/User/flyte/workflows",
                    },
                )
            )
        ) as ctx:
            dynamic_job_spec = my_subwf.compile_into_workflow(ctx, True, my_subwf._task_function, a=5)
            assert len(dynamic_job_spec._nodes) == 5
            assert len(dynamic_job_spec.tasks) == 1
            args = " ".join(dynamic_job_spec.tasks[0].container.args)
            assert args.startswith(
                "pyflyte-fast-execute --additional-distribution s3://my-s3-bucket/fast/123 "
                "--dest-dir /User/flyte/workflows"
            )

    assert context_manager.FlyteContextManager.size() == 1


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
    def dummy_node(node_id) -> Node:
        n = Node(
            node_id,
            metadata=None,
            bindings=[],
            upstream_nodes=[],
            flyte_entity=SQLTask(name="x", query_template="x", inputs={}),
        )

        n._id = node_id
        return n

    px = Promise("x", NodeOutput(var="x", node=dummy_node("n1")))
    py = Promise("y", NodeOutput(var="y", node=dummy_node("n2")))

    def print_expr(expr):
        print(f"{expr} is type {type(expr)}")

    print_expr(px == py)
    print_expr(px < py)
    print_expr((px == py) & (px < py))
    print_expr(((px == py) & (px < py)) | (px > py))
    print_expr(px < 5)
    print_expr(px >= 5)
    print_expr(px != 5)


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
    eval_expr(py != 5, True)
    eval_expr(px != 5, False)


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
    assert context_manager.FlyteContextManager.size() == 1


def test_wf1_branches_ne():
    @task
    def t1(a: int) -> int:
        return a + 1

    @task
    def t2(a: str) -> str:
        return a

    @workflow
    def my_wf(a: int, b: str) -> str:
        new_a = t1(a=a)
        return conditional("test1").if_(new_a != 5).then(t2(a=b)).else_().fail("Unable to choose branch")

    with pytest.raises(ValueError):
        my_wf(a=4, b="hello")

    x = my_wf(a=5, b="hello")
    assert x == "hello"
    assert context_manager.FlyteContextManager.size() == 1


def test_wf1_branches_no_else_malformed_but_no_error():
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

    assert context_manager.FlyteContextManager.size() == 1


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
    assert context_manager.FlyteContextManager.size() == 1


def test_cant_use_normal_tuples():
    with pytest.raises(RestrictedTypeError):

        @task
        def t1(a: str) -> tuple:
            return (a, 3)


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

    serialization_settings = context_manager.SerializationSettings(
        project="proj",
        domain="dom",
        version="123",
        image_config=ImageConfig(Image(name="name", fqn="asdf/fdsa", tag="123")),
        env={},
    )
    lp_model = get_serializable(OrderedDict(), serialization_settings, lp)
    assert len(lp_model.spec.default_inputs.parameters) == 1
    assert lp_model.spec.default_inputs.parameters["a"].required
    assert len(lp_model.spec.fixed_inputs.literals) == 0

    lp_model = get_serializable(OrderedDict(), serialization_settings, lp_with_defaults)
    assert len(lp_model.spec.default_inputs.parameters) == 1
    assert not lp_model.spec.default_inputs.parameters["a"].required
    assert lp_model.spec.default_inputs.parameters["a"].default == _literal_models.Literal(
        scalar=_literal_models.Scalar(primitive=_literal_models.Primitive(integer=3))
    )
    assert len(lp_model.spec.fixed_inputs.literals) == 0

    # Adding a check to make sure oneof is respected. Tricky with booleans... if a default is specified, the
    # required field needs to be None, not False.
    parameter_a = lp_model.spec.default_inputs.parameters["a"]
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
    )

    @workflow
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
        input_data_dir="/var/inputs",
        output_data_dir="/var/outputs",
        inputs=kwtypes(val=int),
        outputs=kwtypes(out=int),
        image="alpine",
        command=["sh", "-c", "echo $(( {{.Inputs.val}} * {{.Inputs.val}} )) | tee /var/outputs/out"],
    )

    sum = ContainerTask(
        name="sum",
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


def test_wf_typed_schema():
    schema1 = FlyteSchema[kwtypes(x=int, y=str)]

    @task
    def t1() -> schema1:
        s = schema1()
        s.open().write(pandas.DataFrame(data={"x": [1, 2], "y": ["3", "4"]}))
        return s

    @task
    def t2(s: FlyteSchema[kwtypes(x=int, y=str)]) -> FlyteSchema[kwtypes(x=int)]:
        df = s.open().all()
        return df[s.column_names()[:-1]]

    @workflow
    def wf() -> FlyteSchema[kwtypes(x=int)]:
        return t2(s=t1())

    w = t1()
    assert w is not None
    df = w.open(override_mode=SchemaOpenMode.READ).all()
    result_df = df.reset_index(drop=True) == pandas.DataFrame(data={"x": [1, 2], "y": ["3", "4"]}).reset_index(
        drop=True
    )
    assert result_df.all().all()

    df = t2(s=w.as_readonly())
    assert df is not None
    result_df = df.reset_index(drop=True) == pandas.DataFrame(data={"x": [1, 2]}).reset_index(drop=True)
    assert result_df.all().all()

    x = wf()
    df = x.open().all()
    result_df = df.reset_index(drop=True) == pandas.DataFrame(data={"x": [1, 2]}).reset_index(drop=True)
    assert result_df.all().all()


def test_wf_schema_to_df():
    schema1 = FlyteSchema[kwtypes(x=int, y=str)]

    @task
    def t1() -> schema1:
        s = schema1()
        s.open().write(pandas.DataFrame(data={"x": [1, 2], "y": ["3", "4"]}))
        return s

    @task
    def t2(df: pandas.DataFrame) -> int:
        return len(df.columns.values)

    @workflow
    def wf() -> int:
        return t2(df=t1())

    x = wf()
    assert x == 2


def test_dict_wf_with_constants():
    @task
    def t1(a: int) -> typing.NamedTuple("OutputsBC", t1_int_output=int, c=str):
        return a + 2, "world"

    @task
    def t2(a: typing.Dict[str, str]) -> str:
        return " ".join([v for k, v in a.items()])

    @workflow
    def my_wf(a: int, b: str) -> (int, str):
        x, y = t1(a=a)
        d = t2(a={"key1": b, "key2": y})
        return x, d

    x = my_wf(a=5, b="hello")
    assert x == (7, "hello world")


def test_dict_wf_with_conversion():
    @task
    def t1(a: int) -> typing.Dict[str, str]:
        return {"a": str(a)}

    @task
    def t2(a: dict) -> str:
        print(f"HAHAH {a}")
        return " ".join([v for k, v in a.items()])

    @workflow
    def my_wf(a: int) -> str:
        return t2(a=t1(a=a))

    with pytest.raises(TypeError):
        my_wf(a=5)


def test_wf_with_empty_dict():
    @task
    def t1() -> typing.Dict:
        return {}

    @task
    def t2(d: typing.Dict):
        assert d == {}

    @workflow
    def wf():
        d = t1()
        t2(d=d)

    wf()


def test_wf_with_catching_no_return():
    @task
    def t1() -> typing.Dict:
        return {}

    @task
    def t2(d: typing.Dict):
        assert d == {}

    @task
    def t3(s: str):
        pass

    with pytest.raises(AssertionError):

        @workflow
        def wf():
            d = t1()
            # The following statement is wrong, this should not be allowed to pass to another task
            x = t2(d=d)
            # Passing x is wrong in this case
            t3(s=x)

        wf()


def test_wf_custom_types_missing_dataclass_json():
    with pytest.raises(AssertionError):

        @dataclass
        class MyCustomType(object):
            pass

        @task
        def t1(a: int) -> MyCustomType:
            return MyCustomType()


def test_wf_custom_types():
    @dataclass_json
    @dataclass
    class MyCustomType(object):
        x: int
        y: str

    @task
    def t1(a: int) -> MyCustomType:
        return MyCustomType(x=a, y="t1")

    @task
    def t2(a: MyCustomType, b: str) -> (MyCustomType, int):
        return MyCustomType(x=a.x, y=f"{a.y} {b}"), 5

    @workflow
    def my_wf(a: int, b: str) -> (MyCustomType, int):
        return t2(a=t1(a=a), b=b)

    c, v = my_wf(a=10, b="hello")
    assert v == 5
    assert c.x == 10
    assert c.y == "t1 hello"


def test_arbit_class():
    class Foo(object):
        pass

    with pytest.raises(ValueError):

        @task
        def t1(a: int) -> Foo:
            return Foo()


def test_dataclass_more():
    @dataclass_json
    @dataclass
    class Datum(object):
        x: int
        y: str
        z: typing.Dict[int, str]

    @task
    def stringify(x: int) -> Datum:
        return Datum(x=x, y=str(x), z={x: str(x)})

    @task
    def add(x: Datum, y: Datum) -> Datum:
        x.z.update(y.z)
        return Datum(x=x.x + y.x, y=x.y + y.y, z=x.z)

    @workflow
    def wf(x: int, y: int) -> Datum:
        return add(x=stringify(x=x), y=stringify(x=y))

    wf(x=10, y=20)


def test_environment():
    @task(environment={"FOO": "foofoo", "BAZ": "baz"})
    def t1(a: int) -> str:
        a = a + 2
        return "now it's " + str(a)

    @workflow
    def my_wf(a: int) -> str:
        x = t1(a=a)
        return x

    serialization_settings = context_manager.SerializationSettings(
        project="test_proj",
        domain="test_domain",
        version="abc",
        image_config=ImageConfig(Image(name="name", fqn="image", tag="name")),
        env={"FOO": "foo", "BAR": "bar"},
    )
    with context_manager.FlyteContextManager.with_context(
        context_manager.FlyteContextManager.current_context().with_new_compilation_state()
    ):
        task_spec = get_serializable(OrderedDict(), serialization_settings, t1)
        assert task_spec.template.container.env == {"FOO": "foofoo", "BAR": "bar", "BAZ": "baz"}


def test_resources():
    @task(requests=Resources(cpu="1"), limits=Resources(cpu="2", mem="400M"))
    def t1(a: int) -> str:
        a = a + 2
        return "now it's " + str(a)

    @task(requests=Resources(cpu="3"))
    def t2(a: int) -> str:
        a = a + 200
        return "now it's " + str(a)

    @workflow
    def my_wf(a: int) -> str:
        x = t1(a=a)
        return x

    serialization_settings = context_manager.SerializationSettings(
        project="test_proj",
        domain="test_domain",
        version="abc",
        image_config=ImageConfig(Image(name="name", fqn="image", tag="name")),
        env={},
    )
    with context_manager.FlyteContextManager.with_context(
        context_manager.FlyteContextManager.current_context().with_new_compilation_state()
    ):
        task_spec = get_serializable(OrderedDict(), serialization_settings, t1)
        assert task_spec.template.container.resources.requests == [
            _resource_models.ResourceEntry(_resource_models.ResourceName.CPU, "1")
        ]
        assert task_spec.template.container.resources.limits == [
            _resource_models.ResourceEntry(_resource_models.ResourceName.CPU, "2"),
            _resource_models.ResourceEntry(_resource_models.ResourceName.MEMORY, "400M"),
        ]

        task_spec2 = get_serializable(OrderedDict(), serialization_settings, t2)
        assert task_spec2.template.container.resources.requests == [
            _resource_models.ResourceEntry(_resource_models.ResourceName.CPU, "3")
        ]
        assert task_spec2.template.container.resources.limits == []


def test_wf_explicitly_returning_empty_task():
    @task
    def t1():
        ...

    @workflow
    def my_subwf():
        return t1()  # This forces the wf _local_execute to handle VoidPromises

    assert my_subwf() is None


def test_nested_dict():
    @task(cache=True, cache_version="1.0.0")
    def squared(value: int) -> typing.Dict[str, int]:
        return {"value:": value ** 2}

    @workflow
    def compute_square_wf(input_integer: int) -> typing.Dict[str, int]:
        compute_square_result = squared(value=input_integer)
        return compute_square_result

    compute_square_wf(input_integer=5)


def test_nested_dict2():
    @task(cache=True, cache_version="1.0.0")
    def squared(value: int) -> typing.List[typing.Dict[str, int]]:
        return [
            {"squared_value": value ** 2},
        ]

    @workflow
    def compute_square_wf(input_integer: int) -> typing.List[typing.Dict[str, int]]:
        compute_square_result = squared(value=input_integer)
        return compute_square_result


def test_secrets():
    @task(secret_requests=[Secret("my_group", "my_key")])
    def foo() -> str:
        return flytekit.current_context().secrets.get("my_group", "")

    with pytest.raises(ValueError):
        foo()

    @task(secret_requests=[Secret("group", group_version="v1", key="key")])
    def foo2() -> str:
        return flytekit.current_context().secrets.get("group", "key")

    os.environ[flytekit.current_context().secrets.get_secrets_env_var("group", "key")] = "super-secret-value2"
    assert foo2() == "super-secret-value2"

    with pytest.raises(AssertionError):

        @task(secret_requests=["test"])
        def foo() -> str:
            pass


def test_nested_dynamic():
    @task
    def t1(a: int) -> str:
        a = a + 2
        return "world-" + str(a)

    @task
    def t2(a: str, b: str) -> str:
        return b + a

    @workflow
    def my_wf(a: int, b: str) -> (str, typing.List[str]):
        @dynamic
        def my_subwf(a: int) -> typing.List[str]:
            s = []
            for i in range(a):
                s.append(t1(a=i))
            return s

        x = t2(a=b, b=b)
        v = my_subwf(a=a)
        return x, v

    v = 5
    x = my_wf(a=v, b="hello ")
    assert x == ("hello hello ", ["world-" + str(i) for i in range(2, v + 2)])

    settings = context_manager.SerializationSettings(
        project="test_proj",
        domain="test_domain",
        version="abc",
        image_config=ImageConfig(Image(name="name", fqn="image", tag="name")),
        env={},
    )

    nested_my_subwf = my_wf.get_all_tasks()[0]

    ctx = context_manager.FlyteContextManager.current_context().with_serialization_settings(settings)
    with context_manager.FlyteContextManager.with_context(ctx) as ctx:
        es = ctx.new_execution_state().with_params(mode=ExecutionState.Mode.TASK_EXECUTION)
        with context_manager.FlyteContextManager.with_context(ctx.with_execution_state(es)) as ctx:
            dynamic_job_spec = nested_my_subwf.compile_into_workflow(ctx, False, nested_my_subwf._task_function, a=5)
            assert len(dynamic_job_spec._nodes) == 5


def test_workflow_named_tuple():
    @task
    def t1() -> str:
        return "Hello"

    @workflow
    def wf() -> typing.NamedTuple("OP", a=str, b=str):
        return t1(), t1()

    assert wf() == ("Hello", "Hello")
