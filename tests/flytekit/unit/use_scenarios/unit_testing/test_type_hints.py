import datetime
import inspect
import typing

import flytekit.annotated.task
import flytekit.annotated.workflow
from flytekit.annotated import context_manager, promise
from flytekit.annotated.context_manager import FlyteContext, ExecutionState
from flytekit.annotated.task import task, AbstractSQLTask, metadata, maptask, dynamic
from flytekit.annotated.workflow import workflow
from flytekit.annotated.interface import extract_return_annotation, transform_variable_map


def test_default_wf_params_works():
    @task
    def my_task(a: int):
        wf_params = flytekit.current_context()
        assert wf_params.execution_id == 'ex:local:local:local'

    my_task(a=3)


def test_simple_input_output():
    @task
    def my_task(a: int) -> typing.NamedTuple("OutputsBC", b=int, c=str):
        ctx = flytekit.current_context()
        assert ctx.execution_id == 'ex:local:local:local'
        return a + 2, "hello world"

    assert my_task(a=3) == (5, 'hello world')


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

    assert my_task() == 'Hello world'

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
    assert result['x_str'].type.simple == 3
    assert result['y_int'].type.simple == 1

    result = transform_variable_map(extract_return_annotation(inspect.signature(y).return_annotation))
    assert result['x_str'].type.simple == 3
    assert result['y_int'].type.simple == 1


def test_unnamed_typing_tuple():
    def z(a: int, b: str) -> typing.Tuple[int, str]:
        return 5, "hello world"

    result = transform_variable_map(extract_return_annotation(inspect.signature(z).return_annotation))
    assert result['out_0'].type.simple == 1
    assert result['out_1'].type.simple == 3


def test_regular_tuple():
    def q(a: int, b: str) -> (int, str):
        return 5, "hello world"

    result = transform_variable_map(extract_return_annotation(inspect.signature(q).return_annotation))
    assert result['out_0'].type.simple == 1
    assert result['out_1'].type.simple == 3


def test_single_output_new_decorator():
    def q(a: int, b: str) -> int:
        return a + len(b)

    result = transform_variable_map(extract_return_annotation(inspect.signature(q).return_annotation))
    assert result['out_0'].type.simple == 1


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

    assert len(my_wf._sdk_workflow.nodes) == 2
    assert my_wf._sdk_workflow.nodes[0].id == "node-0"
    assert my_wf._sdk_workflow.nodes[1]._upstream[0] is my_wf._sdk_workflow.nodes[0]

    assert len(my_wf._sdk_workflow.outputs) == 2
    assert my_wf._sdk_workflow.outputs[0].var == 'out_0'
    assert my_wf._sdk_workflow.outputs[0].binding.promise.var == 't1_int_output'


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
    assert x == {
        'out_0': 7,
        'out_1': "hello world",
    }


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
    assert x == {
        'out_0': 7,
        'out_1': "hello world",
    }


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
        x, y = t1(a=a)
        u, v = my_subwf(a=x)
        return x, u, v

    x = my_wf(a=5, b="hello ")
    assert x == {
        'out_0': 7,
        'out_1': "world-9",
        'out_2': "world-11",
    }


def test_wf1_with_sql():
    sql = AbstractSQLTask(
        "my-query",
        query_template="SELECT * FROM hive.city.fact_airport_sessions WHERE ds = '{{ .Inputs.ds }}' LIMIT 10",
        inputs={"ds": datetime.datetime},
        metadata=metadata(retries=2)
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
    assert x == {
        'out_0': 7,
        'out_1': "hello world",
    }


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
    assert x == {'out_0': 15, 'out_1': 'world-7world-8'}


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
    assert x == {
        'out_0': 7,
        'out_1': "hello This is my way",
    }


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
    assert x == {
        'out_0': "hello hello ",
        'out_1': ["world-" + str(i) for i in range(2, v+2)],
    }

    compiled_sub_wf = my_subwf.compile_into_workflow(a=5)
    assert len(compiled_sub_wf._sdk_workflow.nodes) == 5


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

    assert len(lister._sdk_workflow.outputs) == 1
    binding_data = lister._sdk_workflow.outputs[0].binding  # the property should be named binding_data
    assert binding_data.collection is not None
    assert len(binding_data.collection.bindings) ==  10


# TODO Add an example that shows how tuple fails and it should fail cleanly. As tuple types are not supported!


# @flyte_test
# def test_single_output():
#     @python_task
#     def my_task(ctx) -> typing.BinaryIO:
#         with open("/tmp/blah", mode="w") as fh:
#             fh.writelines("hello world")
#             my_output = upload_to_location(fh, "s3://my-known-location", format="csv")
#             return fh

#     assert my_task.unit_test() == {'output': 'Hello world'}


# @flyte_test
# def test_read_file_known_location():
#     # Option 1
#     # users receive a file handle as the parameter to their task
#     @python_task
#     def my_task(ctx, fh: typing.BinaryIO):
#         lines = fh.readlines()
#         # assert 

#     # Option 1.1
#     # To call the task for unit testing, users need to open a file and pass the handle
#     with open('/mytest', mode='rb') as fh:
#         assert my_task.unit_test(fh=fh) == {'output': 'Hello world'}


#     # Option 1.2
#     # Users pass a Path-Like object that flyte knows how to interpret and open or youo
#     assert my_task.unit_test(fh=CustomPathLike('/mytest', format="csv")) == {}

#     # Option 2
#     # Users receive a Path-Like type as a parameter to their function (much like how Blobs work today)
#     # Option 2.1
#     # Users will have to call downlooad then open
#     @python_task
#     def my_task(ctx, file_path: CustomPathLike):
#         file_path.download()
#         with open(file_path.local_path, mode='rb') as fh:
#             lines = fh.readlines()
#             # assert 

#     assert my_task.unit_test(file_path=CustomPathLike('s3://bucket-my/mytest')) == {}

#     # Option 2.2
#     # Provide a custom open() that encapsulates download and open actions
#     @python_task
#     def my_task(ctx, file_path: CustomPathLike):
#         with file_path.open() as fh:
#             lines = fh.readlines()
#             # assert 


#     # Option 2.3
#     # Flyte always downloads the file and users can use the regular open() function to read it.
#     @python_task
#     def my_task(ctx, file_path: CustomPathLike):
#         with open(file_path.local_path, mode='rb') as fh:
#             lines = fh.readlines()
#             # assert 
# nt1 = typing.NamedTuple("NT1", x_str=str, y_int=int)
