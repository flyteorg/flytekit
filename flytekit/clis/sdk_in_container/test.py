import contextlib
import time
import typing
import rich
import rich.live



@contextlib.contextmanager
def rich_status(l: rich.live.Live, s: str) -> typing.ContextManager["rich.status.Status"]:
    """
    Context manager for rich status logging.

    :return: Status object
    """
    from rich.status import Status
    s = Status(s)
    l.update(s)
    yield s
    l.update("")
    s.stop()


l = rich.live.Live(console=rich.get_console(), refresh_per_second=10)
l.start()
for i in range(10):
    with rich_status(l, f"Working on {i}"):
        time.sleep(0.5)
    rich.print(f"[green][✓][/] {i} Done.")