import pytest
import typing
from io import BufferedWriter

from flytekit.sdk.test_utils import flyte_test
from flytekit.sdk.tasks import python_task
from flytekit.annotated.type_engine import outputs

@flyte_test
def test_simple_input_output():
    @python_task
    def my_task(ctx, a: int) -> outputs(b=int, c=str):
        return a+2, "hello world"
    
    assert my_task.unit_test(a=3) == {'b': 5, 'c': 'hello world'}


@flyte_test
def test_simple_input_no_output():
    @python_task
    def my_task(ctx, a: int):
        pass
    
    assert my_task.unit_test(a=3) == {}


@flyte_test
def test_single_output():
    @python_task
    def my_task(ctx) -> str:
        return "Hello world"
    
    assert my_task.unit_test() == {'output': 'Hello world'}


# @flyte_test
# def test_single_output():
#     @python_task
#     def my_task(ctx) -> typing.BinaryIO:
#         with open("/tmp/blah", mode="w") as fh:
#             fh.writelines("hello world")
#             my_output = upload_to_location(fh, "s3://my-known-location", format="csv")
#             return fh
    
#     assert my_task.unit_test() == {'output': 'Hello world'}


@flyte_test
def test_read_file_known_location():
    @python_task
    def my_task(ctx, fh: typing.BinaryIO):
        lines = fh.readlines()
        # assert 
    
    with open('/mytest', mode='rb') as fh:
        assert my_task.unit_test(fh=fh) == {'output': 'Hello world'}


    assert my_task.unit_test(fh=CustomPathLike('/mytest', format="csv")) == {}
    
    @python_task
    def my_task(ctx, file_path: CustomPathLike):
        file_path.download()
        with open(file_path.local_path, mode='rb') as fh:
            lines = fh.readlines()
            # assert 

    assert my_task.unit_test(file_path=CustomPathLike('s3://bucket-my/mytest')) == {}


    @python_task
    def my_task(ctx, file_path: CustomPathLike):
        with file_path.open() as fh:
            lines = fh.readlines()
            # assert 


    @python_task
    def my_task(ctx, file_path: CustomPathLike):
        with open(file_path.local_path, mode='rb') as fh:
            lines = fh.readlines()
            # assert 
