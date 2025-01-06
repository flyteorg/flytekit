from flytekit import task, workflow
from flytekit.types.file import FlyteFile


@task
def create_ff(file_path: str) -> FlyteFile:
    """Create a FlyteFile."""
    return FlyteFile(path=file_path)


@task
def read_ff(ff: FlyteFile) -> None:
    """Read input FlyteFile.

    This can be used in the case in which a FlyteFile is created
    in another task pod and read in this task pod.
    """
    with open(ff, "r") as f:
        content = f.read()
        print(f"FILE CONTENT | {content}")


@task
def create_and_read_ff(file_path: str) -> FlyteFile:
    """Create a FlyteFile and read it.

    Both FlyteFile creation and reading are done in this task pod.

    Args:
        file_path: File path.

    Returns:
        ff: FlyteFile object.
    """
    ff = FlyteFile(path=file_path)
    with open(ff, "r") as f:
        content = f.read()
        print(f"FILE CONTENT | {content}")

    return ff


@workflow
def wf(remote_file_path: str) -> None:
    ff_1 = create_ff(file_path=remote_file_path)
    read_ff(ff=ff_1)
    ff_2 = create_and_read_ff(file_path=remote_file_path)
    read_ff(ff=ff_2)


if __name__ == "__main__":
    wf()
