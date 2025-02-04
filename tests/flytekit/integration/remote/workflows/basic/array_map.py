import typing
from functools import partial

from flytekit import map, task, workflow


@task
def fn(x: int, y: int) -> int:
    return x + y


@workflow
def workflow_with_maptask(data: typing.List[int], y: int) -> typing.List[int]:
    partial_fn = partial(fn, y=y)
    return map(partial_fn)(x=data)
