"""Script used for testing local execution of nested functions."""

import os

from flytekit import task, workflow


def task_decorator(fn):
    def wrapper(x: int) -> int:
        print("running task_decorator")
        return fn(x=x)

    return wrapper


@task
@task_decorator
def my_task(x: int) -> int:
    print("running my_task")
    return x + 1


@workflow
def my_workflow(x: int) -> int:
    return my_task(x=x)


if __name__ == "__main__":
    print(my_workflow(x=int(os.getenv("SCRIPT_INPUT"))))
