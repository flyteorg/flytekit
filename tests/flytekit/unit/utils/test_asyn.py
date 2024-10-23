import os
import threading
import pytest
import asyncio
from typing import List, Dict, Optional
from asyncio import get_running_loop
from functools import partial
from flytekit.utils.asyn import run_sync, loop_manager

from contextvars import ContextVar


async def async_add(a: int, b: int) -> int:
    return a + b


def test_run_async_function():
    result = run_sync(async_add, a=10, b=12)
    assert result == 22


def sync_sub(a: int, b: int) -> int:
    return a - b


async def async_sub(a: int, b: int) -> int:
    loop = get_running_loop()
    return await loop.run_in_executor(None, partial(sync_sub, a=a, b=b))


def test_run_sync_in_async():
    result = run_sync(async_sub, a=10, b=12)
    assert result == -2


async def async_multiply_inner(a: int, b: int) -> int:
    return a * b


def sync_sub_that_calls_sync(a: int, b: int) -> int:
    return run_sync(async_multiply_inner, a=a, b=b)


async def async_multiply_outer(a: int, b: int) -> int:
    loop = get_running_loop()
    return await loop.run_in_executor(None, partial(sync_sub_that_calls_sync, a=a, b=b))


def test_run_sync_with_nested_async():
    result = run_sync(async_multiply_outer, a=10, b=12)
    assert result == 120


async def an_error():
    raise ValueError


def test_raise_error():
    with pytest.raises(ValueError):
        run_sync(an_error)


class MyContext(object):
    def __init__(self, vals: Optional[Dict[str, int]] = None):
        self.vals = vals


dummy_context: ContextVar[List[MyContext]] = ContextVar("dummy_context", default=[])
dummy_context.set([MyContext(vals={"depth": 0})])


async def async_function(n: int, orig: int) -> str:
    ctx = dummy_context.get()[0]
    print(f"Async[{n}] Started! CTX id {id(ctx)} @ depth {ctx.vals['depth']} Thread: {threading.current_thread().name}")

    if n > 0:
        await asyncio.sleep(0.5)  # Simulate some async work
        result = sync_function(n - 1, orig)  # Call the synchronous function
        return f"Async[{n}]: {result}"
    else:
        print("Async function base case reached")
        result = "async done"

    return f"Async[{n}]: {result}"


async def hello() -> str:
    await asyncio.sleep(0.001)
    return "world"


def sync_function(n: int, orig: int) -> str:
    if n > 0:
        ctx = dummy_context.get()[0]
        d = ctx.vals["depth"]
        print(f"Sync[{n}]: Depth is {d}, increasing to {d + 1} computed {int(orig - n)/2}", flush=True)
        assert d == int((orig - n) / 2)
        ctx.vals["depth"] = d + 1
        if n % 2 == 0:
            result = run_sync(async_function, n - 1, orig)
        else:
            s = loop_manager.synced(async_function)
            result = s(n - 1, orig)

        return f"Sync[{n}]: {result}"
    else:
        print("Sync function base case reached", flush=True)
        return f"Sync[{n}]: done"


def test_recursive_calling():
    main_ctx = dummy_context.get()[0]
    assert main_ctx.vals["depth"] == 0
    sync_function(9, 9)
    # simulate library call
    res = asyncio.run(hello())
    main_ctx.vals["depth"] = 0
    assert res == "world"
    sync_function(6, 6)

    # Check to make sure that the names of the runners have the PID in them. This make the loop manager work with
    # things like pytorch elastic.
    for k in loop_manager._runner_map.keys():
        assert str(os.getpid()) in k
