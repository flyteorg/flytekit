import pytest
import asyncio
from asyncio import get_running_loop
from functools import partial
from flytekit.tools.asyn import sync


async def async_add(a: int, b: int) -> int:
    return a + b


def test_run_async_function():
    result = sync(async_add, a=10, b=12)
    assert result == 22


def sync_sub(a: int, b: int) -> int:
    return a - b


async def async_sub(a: int, b: int) -> int:
    loop = get_running_loop()
    return await loop.run_in_executor(None, partial(sync_sub, a=a, b=b))


def test_run_sync_in_async():
    result = sync(async_sub, a=10, b=12)
    assert result == -2


async def async_multiply_inner(a: int, b: int) -> int:
    return a * b


def sync_sub_that_calls_sync(a: int, b: int) -> int:
    return sync(async_multiply_inner, a=a, b=b)


async def async_multiply_outer(a: int, b: int) -> int:
    loop = get_running_loop()
    return await loop.run_in_executor(None, partial(sync_sub_that_calls_sync, a=a, b=b))


def test_run_sync_with_nested_async():
    result = sync(async_multiply_outer, a=10, b=12)
    assert result == 120


async def an_error():
    raise ValueError


def test_raise_error():
    with pytest.raises(ValueError):
        sync(an_error)
