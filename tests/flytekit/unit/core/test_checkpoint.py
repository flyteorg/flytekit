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


def test_sync_checkpoint_restore(tmpdir):
    td_path = Path(tmpdir)
    dest = td_path.joinpath("dest")
    dest.mkdir()
    src = td_path.joinpath("src")
    src.mkdir()
    prev = src.joinpath("prev")
    p = b"prev-bytes"
    with prev.open("wb") as f:
        f.write(p)
    cp = SyncCheckpoint(checkpoint_dest=str(dest), checkpoint_src=str(src))
    assert cp.read() == p
    assert cp._prev_download_path is not None
    assert cp.restore() == cp._prev_download_path


@flytekit.task
def t1(n: int) -> int:
    ctx = flytekit.current_context()
    cp = ctx.checkpoint
    if cp.prev_exists():
        return n
    cp.write(bytes(n + 1))
    return n + 1


def test_checkpoint_task():
    assert t1(n=5) == 6
