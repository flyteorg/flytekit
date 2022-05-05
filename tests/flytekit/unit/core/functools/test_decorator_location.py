from flytekit import task
from .decorator_source import task_decorator_1, task_decorator_2, task_setup
from flytekit.core.tracker import extract_task_module

@task
@task_decorator_1
@task_decorator_2
def my_task(x: int) -> int:
    print("running my_task")
    return x + 1


@task
@task_setup
def get_data(x: int) -> int:
    print("running get_data")
    return x + 1


def test_fjdskla():
    res = extract_task_module(get_data)
    print(res)
    print(get_data.name)
