import pytest

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
