import tempfile
from pathlib import Path
from typing import Optional

import pytest
from flytekit import task, workflow
from flytekit.types.file import FlyteFile


@pytest.fixture
def local_tmp_txt_files():
    # Create a source file
    with tempfile.NamedTemporaryFile(delete=False, mode="w+", suffix=".txt") as src_file:
        src_file.write("Hello World!")
        src_file.flush()
        src_path = src_file.name

    # Create an empty file as the destination
    with tempfile.NamedTemporaryFile(delete=False, suffix=".txt") as dst_file:
        dst_path = dst_file.name

    yield src_path, dst_path

    # Cleanup
    Path(src_path).unlink(missing_ok=True)
    Path(dst_path).unlink(missing_ok=True)


def test_src_path_with_different_types(local_tmp_txt_files) -> None:

    @task
    def create_flytefile(
        source_path: str,
        use_pathlike_src_path: bool,
        remote_path: Optional[str] = None
    ) -> FlyteFile:
        if use_pathlike_src_path:
            source_path = Path(source_path)
        ff = FlyteFile(path=source_path, remote_path=remote_path)

        return ff

    @workflow
    def wf(
        source_path: str,
        use_pathlike_src_path: bool,
        remote_path: Optional[str] = None
    ) -> FlyteFile:
        return create_flytefile(
            source_path=source_path, use_pathlike_src_path=use_pathlike_src_path, remote_path=remote_path
        )

    def _verify_msg(ff: FlyteFile) -> None:
        with open(ff, "r") as f:
            assert f.read() == "Hello World!"


    source_path, remote_path = local_tmp_txt_files

    # Source path is of type str
    ff_1 = wf(source_path=source_path, use_pathlike_src_path=False, remote_path=None)
    _verify_msg(ff_1)

    ff_2 = wf(source_path=source_path, use_pathlike_src_path=False, remote_path=remote_path)
    _verify_msg(ff_2)

    # Source path is of type pathlib.PosixPath
    ff_3 = wf(source_path=source_path, use_pathlike_src_path=True, remote_path=None)
    _verify_msg(ff_3)

    ff_4 = wf(source_path=source_path, use_pathlike_src_path=True, remote_path=remote_path)
    _verify_msg(ff_4)
