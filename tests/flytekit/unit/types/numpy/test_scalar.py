import numpy as np
from typing_extensions import Annotated

from flytekit import kwtypes, task, workflow


@task
def generate_numpy_int() -> np.integer:
    return np.int32(10)


@task
def generate_numpy_float() -> np.floating:
    return np.float64(3.14)


@task
def generate_mean() -> float:
    x = np.array([1,2,3])
    return x.mean()


@task
def t1(val1: np.integer, val2: np.floating) -> np.floating:
    return val1 + val2


@task
def t2(val1: np.floating, val2: float) -> np.floating:
    return val1 * val2


@workflow
def wf():
    val1 = generate_numpy_int()
    val2 = generate_numpy_float()
    val3 = generate_mean()
    val4 = t1(val1=val1, val2=val2)
    t2(val1=val4, val2=val3)


@workflow
def test_wf():
    wf()
