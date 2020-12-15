import typing

import pytest

from flytekit import task, workflow
from flytekit.annotated.condition import conditional


@task()
def square(n: float) -> float:
    """
    Parameters:
        n (float): name of the parameter for the task will be derived from the name of the input variable
               the type will be automatically deduced to be Types.Integer

    Return:
        float: The label for the output will be automatically assigned and type will be deduced from the annotation

    """
    return n * n


@task()
def double(n: float) -> float:
    """
    Parameters:
        n (float): name of the parameter for the task will be derived from the name of the input variable
               the type will be automatically deduced to be Types.Integer

    Return:
        float: The label for the output will be automatically assigned and type will be deduced from the annotation

    """
    return 2 * n


def test_condition_else_fail():
    @workflow
    def multiplier_2(my_input: float) -> float:
        return (
            conditional("fractions")
            .if_((my_input > 0.1) & (my_input < 1.0))
            .then(double(n=my_input))
            .elif_((my_input > 1.0) & (my_input < 10.0))
            .then(square(n=my_input))
            .else_()
            .fail("The input must be between 0 and 10")
        )

    with pytest.raises(ValueError):
        multiplier_2(my_input=10)


def test_condition_sub_workflows():
    @task()
    def sum_div_sub(a: int, b: int) -> typing.NamedTuple("Outputs", sum=int, div=int, sub=int):
        return a + b, a / b, a - b

    @task()
    def sum_sub(a: int, b: int) -> typing.NamedTuple("Outputs", sum=int, sub=int):
        return a + b, a - b

    @workflow
    def sub_wf(a: int, b: int) -> (int, int):
        return sum_sub(a=a, b=b)

    @workflow
    def math_ops(a: int, b: int) -> (int, int):
        # Flyte will only make `sum` and `sub` available as outputs because they are common between all branches
        sum, sub = (
            conditional("noDivByZero")
            .if_(a > b)
            .then(sub_wf(a=a, b=b))
            .else_()
            .fail("Only positive results are allowed")
        )

        return sum, sub

    x, y = math_ops(a=3, b=2)
    assert x == 5
    assert y == 1


def test_condition_tuple_branches():
    @task()
    def sum_sub(a: int, b: int) -> typing.NamedTuple("Outputs", sum=int, sub=int):
        return a + b, a - b

    @workflow
    def math_ops(a: int, b: int) -> (int, int):
        # Flyte will only make `sum` and `sub` available as outputs because they are common between all branches
        sum, sub = (
            conditional("noDivByZero")
            .if_(a > b)
            .then(sum_sub(a=a, b=b))
            .else_()
            .fail("Only positive results are allowed")
        )

        return sum, sub

    x, y = math_ops(a=3, b=2)
    assert x == 5
    assert y == 1
