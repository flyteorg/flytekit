from flytekit import map_task, task, workflow
import typing


@task
def hello(name: str) -> str:
    return f"Hello {name}"

hello_mapper = map_task(hello)

@workflow
def wf(names: typing.List[str]) -> typing.List[str]:
    return hello_mapper(name=names)
