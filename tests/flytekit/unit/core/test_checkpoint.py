from pathlib import Path

import flytekit
from flytekit.common.tasks.checkpointer import SyncCheckpoint


def test_sync_checkpoint_write(tmpdir):
    td_path = Path(tmpdir)
    cp = SyncCheckpoint(checkpoint_dest=tmpdir)
    assert cp.read() is None
    assert cp.restore() is None
    dst_path = td_path.joinpath(SyncCheckpoint.TMP_DST_PATH)
    assert not dst_path.exists()
    cp.write(b'bytes')
    assert dst_path.exists()


def test_sync_checkpoint_save_file(tmpdir):
    td_path = Path(tmpdir)
    cp = SyncCheckpoint(checkpoint_dest=tmpdir)
    dst_path = td_path.joinpath(SyncCheckpoint.TMP_DST_PATH)
    assert not dst_path.exists()
    inp = td_path.joinpath("test")
    with inp.open("wb") as f:
        f.write(b"blah")
    with inp.open("rb") as f:
        cp.save(f)
    assert dst_path.exists()


def test_sync_checkpoint_save_filepath(tmpdir):
    td_path = Path(tmpdir)
    cp = SyncCheckpoint(checkpoint_dest=tmpdir)
    dst_path = td_path.joinpath("test")
    assert not dst_path.exists()
    inp = td_path.joinpath("test")
    with inp.open("wb") as f:
        f.write(b"blah")
    cp.save(inp)
    assert dst_path.exists()


# TODO To test this locally we have to hijack the call paths, as we will create new sandboxes and hence sandboxes are not repeated
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
    count = 0
    while True:
        try:
            count += 1
            assert t1(n=5) == 6
            break
        except AssertionError as e:
            if count > 3:
                break
            pass

    assert count == 3
