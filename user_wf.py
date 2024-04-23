import typing

from typing_extensions import Annotated  # type: ignore

from flytekit.core.task import task
from flytekit.core.workflow import workflow

nt = typing.NamedTuple("SingleNamedOutput", [("named1", int)])


@task
def t1(a: int) -> nt:
    a = a + 2
    return nt(a)


@workflow
def subwf(a: int) -> nt:
    return t1(a=a)


@workflow
def wf(b: bool) -> nt:
    t = t1(a=b)
    out = subwf(a=b)
    return t1(a=out.named1)

