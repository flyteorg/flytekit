from flytekit.annotated.stuff import task, workflow, WorkflowOutputs
from typing import List
# from flytekit.annotated.code import *
#
#
def identity_fn(x: int) -> int:
    return x


def inverse_fn(x: int) -> int:
    return -x


# @outputs("fico", "msa")
# @task(memory_request='3Gi')
# def simple_task(ctx: flyte.Context) -> (str, int):
#    msa = some_fn()
#    fico = some_other_fn()
#    return fico, msa # Automatically
#
#


# @workflow
# def workflow(in1: datetime.datetime, in2: int) -> WorkflowOutputs:
#     a = task1(in1)
#     b = task2(in2, a)
#     c = task3(task1(b), a, 'hello')
#
#     # This is important - when we're constructing the workflow, we only have access to the things that are passed
#     # in. Will we ever need access to anything not available in the workflow decorator, or in the workflow const
#     # What class is this producing vs what the decorator is producing?
#     return WorkflowOutputs(b, c)


# @workflow
# def BranchNodeExample():
#    ...
#    t1 = task1(x)
#    t2 = flyte.
#    if (t1.v == "val1", then=task2(t1.j)).  # v has to be type str.
#    elif (t1.v == "val2" and t1.v != "val3", then=task3(t1.j)).
#    else (flyte.error("no good option")
#    t5 = task5(t2.ret)
#    # Supported operations are ==, !=, <, <=, >, >= Also Logical and and or
#

@task(outputs=['s_out'])
def x(s: int) -> int:
    return s + 1


@workflow
def my_workflow() -> WorkflowOutputs:
    a = x(s=3)
    b = x(s=a)
    return WorkflowOutputs(b)


# @task(outputs=['list_of_ints', 'nested_list_of_strings'])
# def y(s: int) -> (List[int], List[List[int]]):
#     return (range(0, s), [['hello', 'world'], ['foo', 'bar']])
#
#
# @task(memory_limit='3', outputs=['a', 'b'])
# def z(s: List[int]) -> (int, str):
#     if len(s) % 2 == 0:
#         return 1, 'hi'
#     return 2, 'hi'


# a, b = y(3)
# z(a)


