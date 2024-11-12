import tempfile
from pathlib import Path
from typing import Optional

import pytest
import flytekit
from flytekit import task, workflow
from flytekit.types.directory import FlyteDirectory


N_FILES_PER_DIR = 3

@pytest.fixture
def local_tmp_dirs():
    # Create a source and an empty destination directory
    with (tempfile.TemporaryDirectory() as src_dir,
          tempfile.TemporaryDirectory() as dst_dir):
        for file_idx in range(N_FILES_PER_DIR):
            with open(Path(src_dir) / f"{file_idx}.txt", "w") as f:
                f.write(str(file_idx))

        yield src_dir, dst_dir


def test_src_path_with_different_types(local_tmp_dirs) -> None:

    @task
    def create_flytedir(
        source_path: str,
        use_pathlike_src_path: bool,
        remote_dir: Optional[str] = None
    ) -> FlyteDirectory:
        if use_pathlike_src_path:
            source_path = Path(source_path)
        fd = FlyteDirectory(path=source_path, remote_directory=remote_dir)

        return fd

    @workflow
    def wf(
        source_path: str,
        use_pathlike_src_path: bool,
        remote_dir: Optional[str] = None
    ) -> FlyteDirectory:
        return create_flytedir(
            source_path=source_path, use_pathlike_src_path=use_pathlike_src_path, remote_dir=remote_dir
        )

    def _verify_files(fd: FlyteDirectory) -> None:
        for file_idx in range(N_FILES_PER_DIR):
            with open(fd / f"{file_idx}.txt", "r") as f:
                assert f.read() == str(file_idx)


    source_path, remote_dir = local_tmp_dirs

    # Source path is of type str
    fd_1 = wf(source_path=source_path, use_pathlike_src_path=False, remote_dir=None)
    _verify_files(fd_1)

    fd_2 = wf(source_path=source_path, use_pathlike_src_path=False, remote_dir=remote_dir)
    _verify_files(fd_2)

    # Source path is of type pathlib.PosixPath
    fd_3 = wf(source_path=source_path, use_pathlike_src_path=True, remote_dir=None)
    _verify_files(fd_3)

    fd_4 = wf(source_path=source_path, use_pathlike_src_path=True, remote_dir=remote_dir)
    _verify_files(fd_4)
