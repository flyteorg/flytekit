import inspect
import typing

import flytekit
import flytekit.annotated.task
import flytekit.annotated.workflow
from flytekit.annotated import context_manager
from flytekit.annotated.task import task
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

    # ctx = context_manager.FlyteContext.current_context()
    # with ctx.new_execution_context(mode=context_manager.ExecutionState.Mode.LOCAL_WORKFLOW_EXECUTION,
    #                                cloud_provider="") as ctx:
    #     result = my_task(a=3)
    #     print(result)


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
        assert outputs.sdk_node is nodes[0]


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

# def test_normal_path():
#     # Write some random numbers to a file
#     def t1():
#         ...
#
#     # Read back the file and transform it
#     def t2(in1: _flytekit_typing.FlyteFilePath) -> str:
#         with open(in1, 'r') as fh:
#             lines = fh.readlines()
#             return "".join(lines)
#
#
#
#


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
