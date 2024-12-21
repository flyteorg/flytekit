from flytekit import task, workflow
from flytekit.types.file import FlyteFile


@task
def open_ff_from_remote(remote_file_path: str) -> FlyteFile:
    """Open FlyteFile from a remote file path.

    Args:
        remote_file_path: Remote file path.

    Returns:
        ff: FlyteFile object.
    """
    ff = FlyteFile(path=remote_file_path)
    with open(ff, "r") as f:
        content = f.read()
        print(f"FILE CONTENT | {content}")

    return ff


@workflow
def wf(remote_file_path: str) -> None:
    remote_ff = open_ff_from_remote(remote_file_path=remote_file_path)


if __name__ == "__main__":
    wf()
