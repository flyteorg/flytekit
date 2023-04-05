import random
from typing import Tuple

from flytekit import task, workflow


@task
def add_rand(n: int) -> float:
    return float(n + random.randint(-1000, 1000))


@task
def bad_types(a: int) -> float:
    return str(a)


@task
def good_types(a: float) -> str:
    return str(a)


@workflow
def wf(a: int, b: int):
    bad_types(a=add_rand(n=a))


@workflow
def wf_bad_output(a: int, b: int) -> int:
    return good_types(a=add_rand(n=a))


@workflow
def wf_bad_multioutput1(a: int, b: int) -> Tuple[int, str]:
    out1 = good_types(a=add_rand(n=a))
    out2 = good_types(a=add_rand(n=b))
    return out1, out2


@workflow
def wf_bad_multioutput2(a: int, b: int) -> Tuple[str, int]:
    out1 = good_types(a=add_rand(n=a))
    out2 = good_types(a=add_rand(n=b))
    return out1, out2


if __name__ == "__main__":
    # wf(a=1, b=1)
    # wf(a=1, b=1.0)
    # wf_bad_output(a=1, b=1)
    # wf_bad_multioutput(a=1, b=2)
    # wf_bad_multioutput2(a=1, b=2)
    bad_types(a=1)
    # bad_types(a=str(1))
