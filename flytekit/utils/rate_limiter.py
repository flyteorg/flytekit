import asyncio
from collections import deque
from datetime import datetime, timedelta

from flytekit.loggers import developer_logger
from flytekit.utils.asyn import run_sync


class RateLimiter:
    """Rate limiter that allows up to a certain number of requests per minute."""

    def __init__(self, rpm: int):
        if not isinstance(rpm, int) or rpm <= 0 or rpm > 100:
            raise ValueError("Rate must be a positive integer between 1 and 100")
        self.rpm = rpm
        self.queue = deque()
        self.sem = asyncio.Semaphore(rpm)
        self.delay = timedelta(seconds=60)  # always 60 seconds since this we're using a per-minute rate limiter

    def sync_acquire(self):
        run_sync(self.acquire)

    async def acquire(self):
        async with self.sem:
            now = datetime.now()
            # Start by clearing out old data
            while self.queue and (now - self.queue[0]) > self.delay:
                self.queue.popleft()

            # Now that the queue only has valid entries, we'll need to wait if the queue is full.
            if len(self.queue) >= self.rpm:
                # Compute necessary delay and sleep that amount
                # First pop one off, so another coroutine won't try to base its wait time off the same timestamp. But
                # if you pop it off, the next time this code runs it'll think there's enough spots... so add the
                # expected time back onto the queue before awaiting. Once you await, you lose the 'thread' and other
                # coroutines can run.
                # Basically the invariant is: this block of code leaves the number of items in the queue unchanged:
                # it'll pop off a timestamp but immediately add one back.
                # Because of the semaphore, we don't have to worry about the one we add to the end being referenced
                # because there will never be more than RPM-1 other coroutines running at the same time.
                earliest = self.queue.popleft()
                delay: timedelta = (earliest + self.delay) - now
                if delay.total_seconds() > 0:
                    next_time = earliest + self.delay
                    self.queue.append(next_time)
                    developer_logger.debug(
                        f"Capacity reached - removed time {earliest} and added back {next_time}, sleeping for {delay.total_seconds()}"
                    )
                    await asyncio.sleep(delay.total_seconds())
                else:
                    developer_logger.debug(f"No more need to wait, {earliest=} vs {now=}")
                    self.queue.append(now)
            else:
                self.queue.append(now)
