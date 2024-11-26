import asyncio
import typing
import pytest

from flytekit.core.task import task, eager
from flytekit.configuration import Config
from flytekit.core.context_manager import FlyteContextManager
from flytekit.core.data_persistence import FileAccessProvider
from flytekit.remote.remote import FlyteRemote
from flytekit.utils.asyn import loop_manager


@task
def add_one(x: int) -> int:
    return x + 1


@task
async def a_double(x: int) -> int:
    return x * 2


@task
def double(x: int) -> int:
    return x * 2


@eager
async def base_wf(x: int) -> int:
    out = add_one(x=x)
    doubled = a_double(x=x)
    if out - await doubled < 0:
        return -1
    final = double(x=out)
    return final


@eager
async def parent_wf(a: int, b: int) -> typing.Tuple[int, int]:
    print("hi")
    t1 = asyncio.create_task(base_wf(x=a))
    t2 = asyncio.create_task(base_wf(x=b))
    # Test this again in the future
    # currently behaving as python does.
    # print("hi2", flush=True)
    # await asyncio.sleep(0.01)
    # time.sleep(5)
    # Since eager workflows are also async tasks, we can use the general async pattern with them.
    i1, i2 = await asyncio.gather(t1, t2)
    return i1, i2


@pytest.mark.asyncio
async def test_nested_all_local():
    res = await parent_wf(a=1, b=2)
    print(res)
    assert res == (4, -1)


@pytest.mark.skip
def test_nested_local_backend():
    ctx = FlyteContextManager.current_context()
    remote = FlyteRemote(Config.for_sandbox())
    dc = Config.for_sandbox().data_config
    raw_output = f"s3://my-s3-bucket/testing/async_test/raw_output/"
    print(f"Using raw output location: {raw_output}")
    provider = FileAccessProvider(local_sandbox_dir="/tmp/unittest", raw_output_prefix=raw_output, data_config=dc)

    with FlyteContextManager.with_context(ctx.with_file_access(provider).with_client(remote.client)):
        res = loop_manager.run_sync(parent_wf.run_with_backend, a=1, b=100)
        print(res)
        # Nested eagers just run against the backend like any other task.
        assert res == (42, 44)


@eager
async def level_3(x: int) -> int:
    out = add_one(x=x)
    return out


@eager
async def level_2(x: int) -> int:
    out = add_one(x=x)
    level_3_res = await level_3(x=out)
    final_res = double(x=level_3_res)
    return final_res


@eager
async def level_1() -> typing.Tuple[int, int]:
    i1 = add_one(x=5)
    t2 = asyncio.create_task(level_2(x=1))

    # don't forget the comma
    i2, = await asyncio.gather(t2)
    return i1, i2


@pytest.mark.asyncio
async def test_nested_level_1_local():
    res = await level_1()
    print(res)
    assert res == (6, 6)


@pytest.mark.skip
def test_nested_local_backend_level():
    ctx = FlyteContextManager.current_context()
    remote = FlyteRemote(Config.for_sandbox())
    dc = Config.for_sandbox().data_config
    raw_output = f"s3://my-s3-bucket/testing/async_test/raw_output/"
    print(f"Using raw output location: {raw_output}")
    provider = FileAccessProvider(local_sandbox_dir="/tmp/unittest", raw_output_prefix=raw_output, data_config=dc)

    with FlyteContextManager.with_context(ctx.with_file_access(provider).with_client(remote.client)):
        res = loop_manager.run_sync(level_1.run_with_backend)
        print(res)
        assert res == (42, 42)
