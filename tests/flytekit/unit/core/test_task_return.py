import pytest
from flytekit import task
from flytekit.exceptions.user import FlyteValueException
from typing import Optional


def test_task_return():
    @task
    def foo(a: int) -> int:
        return a + 1

    assert foo(1) == 2


def test_task_optional_return():
    @task
    def foo(return_none: bool) -> Optional[int]:
        return None if return_none else 1

    assert foo(True) is None
    assert foo(False) == 1


def test_task_no_return():
    @task
    def foo(a: int):
        return a + 1

    with pytest.raises(
        FlyteValueException,
        match="Interface has 0 outputs.",
    ):
        foo(1)
