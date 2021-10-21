"""Script used for testing local execution of functool.wraps-wrapped tasks"""

from functools import wraps

from flytekit.core.task import task
from flytekit.core.workflow import workflow


def task_decorator(fn):
    @wraps(fn)
    def wrapper(*args, **kwargs):
        print(f"before running {fn.__name__}")
        try:
            print(f"try running {fn.__name__}")
            out = fn(*args, **kwargs)
        except Exception as exc:
            print(f"error running {fn.__name__}: {exc}")
        finally:
            print(f"finally after running {fn.__name__}")
        print(f"after running {fn.__name__}")
        return out

    return wrapper


def bleh(some_compile_time_config):
    @task
    @task_decorator
    def inner_task_1(x: int) -> int:
        assert x > 0, f"my_task failed with input: {x}"
        print("running my_task")
        return x + 1

    @task
    @task_decorator
    def inner_task_2(x: int) -> int:
        assert x > 0, f"my_task failed with input: {x}"
        print("running my_task")
        return x + 1

    if some_compile_time_config == 1:
        return inner_task_1

    return inner_task_2


t = bleh(2)


@workflow
def my_workflow2(x: int) -> int:
    return t(x=x)

