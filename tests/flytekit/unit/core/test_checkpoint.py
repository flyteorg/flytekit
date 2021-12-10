from pathlib import Path

import flytekit


@flytekit.task
def t1(n: int) -> int:
    ctx = flytekit.current_context()
    cp = ctx.checkpointer
    m = 0
    if cp.prev_exists():
        v = cp.read()
        m = int(v)
        if m == 3:
            return n + 1
    cp.write(bytes(m + 1))
    raise AssertionError("Fail!")


def test_checkpoint_task():
    pass
