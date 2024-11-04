from pathlib import Path
from typing import Optional

import flytekit
from flytekit import task, workflow
from flytekit.types.directory import FlyteDirectory


def test_src_path_with_different_types() -> None:
    N_FILES = 3

    @task
    def write_fidx_task(
        use_str_src_path: bool, remote_dir: Optional[str] = None
    ) -> FlyteDirectory:
        """Write file indices to text files in a source path."""
        source_path = Path(flytekit.current_context().working_directory) / "txt_files"
        source_path.mkdir(exist_ok=True)

        for file_idx in range(N_FILES):
            file_path = source_path / f"{file_idx}.txt"
            with file_path.open(mode="w") as f:
                f.write(str(file_idx))

        if use_str_src_path:
            source_path = str(source_path)
        fd = FlyteDirectory(path=source_path, remote_directory=remote_dir)

        return fd

    @workflow
    def wf(use_str_src_path: bool, remote_dir: Optional[str] = None) -> FlyteDirectory:
        return write_fidx_task(use_str_src_path=use_str_src_path, remote_dir=remote_dir)

    def _verify_files(fd: FlyteDirectory) -> None:
        for file_idx in range(N_FILES):
            with open(fd / f"{file_idx}.txt", "r") as f:
                assert f.read() == str(file_idx)

    # Source path is of type str
    ff_1 = wf(use_str_src_path=True, remote_dir=None)
    _verify_files(ff_1)

    ff_2 = wf(use_str_src_path=True, remote_dir="./my_txt_files")
    _verify_files(ff_2)

    # Source path is of type pathlib.PosixPath
    ff_3 = wf(use_str_src_path=False, remote_dir=None)
    _verify_files(ff_3)

    ff_4 = wf(use_str_src_path=False, remote_dir="./my_txt_files2")
    _verify_files(ff_4)
