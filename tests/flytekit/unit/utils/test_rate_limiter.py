import pytest
import sys
import timeit
import asyncio

from datetime import timedelta
from flytekit.utils.rate_limiter import RateLimiter


async def launch_requests(rate_limiter: RateLimiter, total: int):
    tasks = [asyncio.create_task(rate_limiter.acquire()) for _ in range(total)]
    await asyncio.gather(*tasks)


async def helper_for_async(rpm: int, total: int):
    rate_limiter = RateLimiter(rpm=rpm)
    rate_limiter.delay = timedelta(seconds=1)
    await launch_requests(rate_limiter, total)


def runner_for_async(rpm: int, total: int):
    loop = asyncio.get_event_loop()
    return loop.run_until_complete(helper_for_async(rpm, total))


@pytest.mark.asyncio
def test_rate_limiter():
    elapsed_time = timeit.timeit(lambda: runner_for_async(2, 2), number=1)
    elapsed_time_more = timeit.timeit(lambda: runner_for_async(2, 6), number=1)
    assert elapsed_time < 0.25
    assert round(elapsed_time_more) == 2


async def sync_wrapper(rate_limiter: RateLimiter):
    rate_limiter.sync_acquire()


async def helper_for_sync(rpm: int, total: int):
    rate_limiter = RateLimiter(rpm=rpm)
    rate_limiter.delay = timedelta(seconds=1)
    tasks = [asyncio.create_task(sync_wrapper(rate_limiter)) for _ in range(total)]
    await asyncio.gather(*tasks)


def runner_for_sync(rpm: int, total: int):
    loop = asyncio.get_event_loop()
    return loop.run_until_complete(helper_for_sync(rpm, total))


@pytest.mark.asyncio
def test_rate_limiter_s():
    elapsed_time = timeit.timeit(lambda: runner_for_sync(2, 2), number=1)
    elapsed_time_more = timeit.timeit(lambda: runner_for_sync(2, 6), number=1)
    assert elapsed_time < 0.25
    assert round(elapsed_time_more) == 2
