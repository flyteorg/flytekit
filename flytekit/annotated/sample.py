import typing

from flytekit.annotated.stuff import task


foo_int = typing.NamedTuple("foo_int", foo_int=int)


@task
def x(s: int) -> foo_int:
    return s + 1,

