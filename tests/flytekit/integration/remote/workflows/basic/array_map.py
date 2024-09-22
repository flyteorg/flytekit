import typing
from functools import partial
import os

from flytekit import map_task, task, workflow

IMAGE = os.environ.get("FLYTEKIT_IMAGE", "localhost:30000/flytekit:dev")


@task(container_image=IMAGE)
def fn(x: int, y: int) -> int:
    return x + y


@workflow
def workflow_with_maptask(data: typing.List[int], y: int) -> typing.List[int]:
    partial_fn = partial(fn, y=y)
    return map_task(partial_fn)(x=data)
