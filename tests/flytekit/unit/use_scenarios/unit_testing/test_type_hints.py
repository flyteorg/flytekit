import pytest
import typing
from io import BufferedWriter
from datetime import timedelta

from flytekit.sdk.test_utils import flyte_test
from flytekit.sdk.tasks import python_task, current_context, inputs
from flytekit.sdk.types import Types
from flytekit.annotated import type_engine
from flytekit import typing as flytekit_typing
from flytekit.models import types as model_types


@flyte_test
def test_old_style_task():
    @inputs(a=Types.Integer)
    @python_task
    def my_task(wf_params, a):
        print(wf_params)
        print(a)
        assert wf_params.execution_id == 'ex:unit_test:unit_test:unit_test'

    assert my_task.unit_test(a=3) == {}


@flyte_test
def test_simple_input_output():
    @python_task
    def my_task(a: int) -> typing.NamedTuple("OutputsBC", b=int, c=str):
        ctx = current_context()
        print(ctx)
        assert ctx.execution_id == 'ex:unit_test:unit_test:unit_test'
        return a+2, "hello world"

    assert my_task.unit_test(a=3) == {'b': 5, 'c': 'hello world'}


@flyte_test
def test_simple_input_no_output():
    @python_task
    def my_task(a: int):
        pass
    
    assert my_task.unit_test(a=3) == {}


@flyte_test
def test_single_output():
    @python_task
    def my_task() -> str:
        return "Hello world"
    
    assert my_task.unit_test() == {'output': 'Hello world'}


# def test_normal_path():
#     def t1(in1: flytekit_typing.FlyteFilePath) -> str:
#         with open(in1, 'r') as fh:
#             lines = fh.readlines()
#             return "".join(lines)

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
