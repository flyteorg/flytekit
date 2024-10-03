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
