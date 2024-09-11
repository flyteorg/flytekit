import asyncio
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
from signal import SIGINT, SIGTERM

from flytekit.loggers import logger


def handler(loop, s: int):
    loop.stop()
    logger.debug(f"Shutting down loop at {id(loop)} via {s!s}")
    loop.remove_signal_handler(SIGTERM)
    loop.add_signal_handler(SIGINT, lambda: None)


@contextmanager
def use_event_loop():
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    executor = ThreadPoolExecutor()
    loop.set_default_executor(executor)
    for sig in (SIGTERM, SIGINT):
        loop.add_signal_handler(sig, handler, loop, sig)
    try:
        yield loop
    finally:
        tasks = asyncio.all_tasks(loop=loop)
        for t in tasks:
            logger.debug(f"canceling {t.get_name()}")
            t.cancel()
        group = asyncio.gather(*tasks, return_exceptions=True)
        loop.run_until_complete(group)
        executor.shutdown(wait=True)
        loop.close()
