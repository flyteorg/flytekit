import typing

from flytekit.annotated.stuff import task


foo_int = typing.NamedTuple("foo_int", foo_int=int)


@task
def x(s: int) -> foo_int:
    return s + 1,


# @workflow
# def my_workflow() -> outputs(real_b=int):
#     # a = x(s=3)
#     b = x(s=x(s=3))
#     return WorkflowOutputs(b)
