import typing
from functools import partial

from flytekit import map_task, task, workflow, dynamic


@task
def get_static_list() -> typing.List[float]:
    return [3.14, 2.718]


@task
def t3(a: int, b: str, c: typing.List[float], d: typing.List[float]) -> str:
    return str(a) + b + str(c) + "&&" + str(d)


t3_bind_b1 = partial(t3, b="hello")
t3_bind_b2 = partial(t3, b="world")
t3_bind_c1 = partial(t3_bind_b1, c=[6.674, 1.618, 6.626], d=[1.])

mt1 = map_task(t3_bind_c1)


@task
def print_lists(i: typing.List[str], j: typing.List[str]) -> str:
    print(f"First: {i}")
    print(f"Second: {j}")
    return f"{i}-{j}"


@dynamic
def dt1(a: typing.List[int], sl: typing.List[float]) -> str:
    i = mt1(a=a)
    t3_bind_c2 = partial(t3_bind_b2, c=[1., 2., 3.], d=sl)
    mt_in2 = map_task(t3_bind_c2)
    j = mt_in2(a=[3, 4, 5])
    return print_lists(i=i, j=j)


@workflow
def wf_dt(a: typing.List[int]) -> str:
    sl = get_static_list()
    return dt1(a=a, sl=sl)
