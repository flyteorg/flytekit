import contextlib
import os
import tempfile
import typing
from pathlib import Path

from flytekit import FlyteContextManager


class Checkpointer(object):
    SRC_LOCAL_PATH = "_src_cp"

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
            for f in src_cp.glob("*"):
                if f.is_dir():
                    continue
                with open(f, "rb") as b:
                    yield b
        return

    def restore(self, path: Path) -> bool:
        if self._checkpoint_src is None:
            return False
        FlyteContextManager.current_context().file_access.download_directory(self._checkpoint_src, path.name)
        return True

    def save(self, cp: typing.Union[Path, str, typing.BinaryIO]):
        fa = FlyteContextManager.current_context().file_access
        if isinstance(cp, Path) or isinstance(cp, str):
            if isinstance(cp, str):
                cp = Path(cp)
            if cp.is_dir():
                fa.upload_directory(cp.name, self._checkpoint_dest)
            else:
                fname = cp.stem + cp.suffix
                # TODO path constructor needed
                rpath = fa.construct_random_path(self._checkpoint_dest, fname)
                fa.upload_directory(cp.name, rpath)
        if not isinstance(cp, typing.BinaryIO):
            raise ValueError("Only a valid path or BinaryIO should be provided")
        # TODO upload file
        pass