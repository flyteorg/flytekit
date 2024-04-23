
from flytekit import task, workflow


@task
def say_hello() -> str:
    return "Hello, World!"


@workflow
def hello_world_wf() -> str:
    res = say_hello()
    return res

