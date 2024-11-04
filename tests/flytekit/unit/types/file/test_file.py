from pathlib import Path
from typing import Optional

import flytekit
from flytekit import task, workflow
from flytekit.types.file import FlyteFile



def test_src_path_with_different_types() -> None:
    MSG = "Hello!"

    @task
    def write_hello_task(
            use_str_src_path: bool,
            remote_path: Optional[str] = None
    ) -> FlyteFile:
        """Write a greeting message to the source path."""
        source_path = Path(flytekit.current_context().working_directory) / "hello.txt"
        if use_str_src_path:
            source_path = str(source_path)
            f = open(source_path, mode="w")
        else:
            f = source_path.open(mode="w")
        f.write(MSG)
        f.close()

        ff = FlyteFile(path=source_path, remote_path=remote_path)

        return ff

    @workflow
    def wf(
            use_str_src_path: bool,
            remote_path: Optional[str] = None
    ) -> FlyteFile:
        return write_hello_task(use_str_src_path=use_str_src_path, remote_path=remote_path)

    def _verify_msg(msg: str) -> None:
        assert msg == MSG

    # Source path is of type str
    ff_1 = wf(use_str_src_path=True, remote_path=None)
    with open(ff_1, "r") as f:
        _verify_msg(f.read())

    ff_2 = wf(use_str_src_path=True, remote_path="./my_hello.txt")
    with open(ff_2, "r") as f:
        _verify_msg(f.read())


    # Source path is of type pathlib.PosixPath
    ff_3 = wf(use_str_src_path=False, remote_path=None)
    with open(ff_3, "r") as f:
        _verify_msg(f.read())

    ff_4 = wf(use_str_src_path=False, remote_path="./my_hello.txt")
    with open(ff_4, "r") as f:
        _verify_msg(f.read())
