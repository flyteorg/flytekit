import contextlib
import io
import tempfile
import typing
from pathlib import Path

from flytekit import FlyteContextManager


class SyncCheckpoint(object):
    """
    Sync Checkpoint, will synchronously checkpoint a user given file or folder.
    It will also synchronously download / restore previous checkpoints
    """
    SRC_LOCAL_PATH = "_src_cp"
    TMP_DST_PATH = "_dst_cp"

    def __init__(self, checkpoint_dest: str, checkpoint_src: typing.Optional[str] = None):
        self._checkpoint_dest = checkpoint_dest
        self._checkpoint_src = checkpoint_src
        self._td = tempfile.TemporaryDirectory()
        self._downloaded = False

    def __del__(self):
        self._td.cleanup()

    @contextlib.contextmanager
    def open(self) -> typing.Iterator[typing.Optional[typing.BinaryIO]]:
        if self._checkpoint_src is None:
            yield None
            return
        p = Path(self._td.name)
        src_cp = p.joinpath(self.SRC_LOCAL_PATH)
        FlyteContextManager.current_context().file_access.download_directory(self._checkpoint_src, src_cp.name)
        if src_cp.is_dir():
            for f in src_cp.iterdir():
                if f.is_dir():
                    continue
                with f.open("rb") as b:
                    yield b
        return

    def restore(self, path: typing.Union[Path, str]) -> bool:
        if self._checkpoint_src is None:
            return False
        if isinstance(path, str):
            path = Path(path)
        FlyteContextManager.current_context().file_access.download_directory(self._checkpoint_src, path.name)
        return True

    def save(self, cp: typing.Union[Path, str, io.BufferedReader]):
        fa = FlyteContextManager.current_context().file_access
        if isinstance(cp, Path) or isinstance(cp, str):
            if isinstance(cp, str):
                cp = Path(cp)
            if cp.is_dir():
                fa.upload_directory(str(cp), self._checkpoint_dest)
            else:
                fname = cp.stem + cp.suffix
                rpath = fa._default_remote.construct_path(False, False, self._checkpoint_dest, fname)
                fa.upload(str(cp), rpath)
            return

        if not isinstance(cp, io.BufferedReader):
            raise ValueError(f"Only a valid path or BufferedReader should be provided, received {type(cp)}")

        p = Path(self._td.name)
        dest_cp = p.joinpath(self.TMP_DST_PATH)
        with dest_cp.open("wb") as f:
            f.write(cp.read())

        rpath = fa._default_remote.construct_path(False, False, self._checkpoint_dest, self.TMP_DST_PATH)
        fa.upload(str(dest_cp), rpath)
