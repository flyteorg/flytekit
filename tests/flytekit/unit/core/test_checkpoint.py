from pathlib import Path

import pytest

import flytekit
from flytekit.core.checkpointer import SyncCheckpoint
from flytekit.core.local_cache import LocalTaskCache


def test_sync_checkpoint_write(tmpdir):
    td_path = Path(tmpdir)
    cp = SyncCheckpoint(checkpoint_dest=tmpdir)
    assert cp.read() is None
    assert cp.restore() is None
    dst_path = td_path.joinpath(SyncCheckpoint.TMP_DST_PATH)
    assert not dst_path.exists()
    cp.write(b"bytes")
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

    with pytest.raises(ValueError):
        # Unsupported object
        cp.save(SyncCheckpoint)  # noqa


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
    user_dest = td_path.joinpath("user_dest")

    with pytest.raises(ValueError):
        cp.restore(user_dest)

    user_dest.mkdir()
    assert cp.restore(user_dest) == user_dest
    assert cp.restore("other_path") == user_dest


def test_sync_checkpoint_restore_default_path(tmpdir):
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


def test_sync_checkpoint_read_empty_dir(tmpdir):
    td_path = Path(tmpdir)
    dest = td_path.joinpath("dest")
    dest.mkdir()
    src = td_path.joinpath("src")
    src.mkdir()
    cp = SyncCheckpoint(checkpoint_dest=str(dest), checkpoint_src=str(src))
    assert cp.read() is None


def test_sync_checkpoint_read_multiple_files(tmpdir):
    """
    Read can only work with one file.
    """
    td_path = Path(tmpdir)
    dest = td_path.joinpath("dest")
    dest.mkdir()
    src = td_path.joinpath("src")
    src.mkdir()
    prev = src.joinpath("prev")
    prev2 = src.joinpath("prev2")
    p = b"prev-bytes"
    with prev.open("wb") as f:
        f.write(p)
    with prev2.open("wb") as f:
        f.write(p)
    cp = SyncCheckpoint(checkpoint_dest=str(dest), checkpoint_src=str(src))

    with pytest.raises(ValueError, match="Expected exactly one checkpoint - found 2"):
        cp.read()


@flytekit.task
def t1(n: int) -> int:
    ctx = flytekit.current_context()
    cp = ctx.checkpoint
    cp.write(bytes(n + 1))
    return n + 1


@flytekit.task(cache=True, cache_version="v0")
def t2(n: int) -> int:
    ctx = flytekit.current_context()
    cp = ctx.checkpoint
    cp.write(bytes(n + 1))
    return n + 1


@pytest.fixture(scope="function", autouse=True)
def setup():
    LocalTaskCache.initialize()
    LocalTaskCache.clear()


def test_checkpoint_task():
    assert t1(n=5) == 6


def test_checkpoint_cached_task():
    assert t2(n=5) == 6
