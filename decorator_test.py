import flytekit
from flytekit import task, workflow
from flytekit.core.node_creation import create_node

from functools import partial, wraps


def task_decorator(fn):
    @wraps(fn)
    def wrapper(*args, **kwargs):
        return fn(*args, **kwargs)

    return wrapper


@task
def setup():
    print("setting up workflow")


@task
def teardown():
    print("tearing down workflow")


def workflow_decorator(fn=None, *, before, after):
    @wraps(fn)
    def wrapper(*args, **kwargs):
        create_node(before)
        outputs = fn(*args, **kwargs)
        create_node(after)
        return outputs

    if fn is None:
        return partial(workflow_decorator, before=before, after=after)

    return wrapper


@task
@task_decorator
def my_task(x: int) -> int:
    print("running my_task")
    return x + 1


@task
def another_task(x: int) -> int:
    print("running another_task")
    return x * 2


@workflow
@workflow_decorator(before=setup, after=teardown)
def my_workflow(x: int) -> int:
    return another_task(x=my_task(x=x))


if __name__ == "__main__":
    print(my_workflow(x=10))


# output:
# setting up workflow
# running my_task
# running another_task
# tearing down workflow
# 22
