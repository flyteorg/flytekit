import pytest

from flytekit import workflow, task
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
