import datetime
import os
import sys
from pathlib import Path
from typing import Tuple

import docker
import pytest

import flytekit
from flytekit import ContainerTask, kwtypes, task, workflow
from flytekit.types.directory import FlyteDirectory
from flytekit.types.file import FlyteFile


@pytest.mark.skipif(
    sys.platform in ["darwin", "win32"],
    reason="Skip if running on windows or macos due to CI Docker environment setup failure",
)
def test_flytefile_wf():
    client = docker.from_env()
    path_to_dockerfile = "tests/flytekit/unit/core/"
    dockerfile_name = "Dockerfile.raw_container"
    client.images.build(path=path_to_dockerfile, dockerfile=dockerfile_name, tag="flytekit:rawcontainer")

    flyte_file_io = ContainerTask(
        name="flyte_file_io",
        input_data_dir="/var/inputs",
        output_data_dir="/var/outputs",
        inputs=kwtypes(inputs=FlyteFile),
        outputs=kwtypes(out=FlyteFile),
        image="flytekit:rawcontainer",
        command=[
            "python",
            "write_flytefile.py",
            "/var/inputs/inputs",
            "/var/outputs/out",
        ],
    )

    @task
    def flyte_file_task() -> FlyteFile:
        working_dir = flytekit.current_context().working_directory
        write_file = os.path.join(working_dir, "flyte_file.txt")
        with open(write_file, "w") as file:
            file.write("This is flyte_file.txt file.")
        return FlyteFile(path=write_file)

    @workflow
    def flyte_file_io_wf() -> FlyteFile:
        ff = flyte_file_task()
        return flyte_file_io(inputs=ff)

    flytefile = flyte_file_io_wf()
    assert isinstance(flytefile, FlyteFile)

    with open(flytefile.path, "r") as file:
        content = file.read()

    assert content == "This is flyte_file.txt file."


@pytest.mark.skipif(
    sys.platform in ["darwin", "win32"],
    reason="Skip if running on windows or macos due to CI Docker environment setup failure",
)
def test_flytefile_wrong_syntax():
    client = docker.from_env()
    path_to_dockerfile = "tests/flytekit/unit/core/"
    dockerfile_name = "Dockerfile.raw_container"
    client.images.build(path=path_to_dockerfile, dockerfile=dockerfile_name, tag="flytekit:rawcontainer")

    flyte_file_io = ContainerTask(
        name="flyte_file_io",
        input_data_dir="/var/inputs",
        output_data_dir="/var/outputs",
        inputs=kwtypes(inputs=FlyteFile),
        outputs=kwtypes(out=FlyteFile),
        image="flytekit:rawcontainer",
        command=[
            "python",
            "write_flytefile.py",
            "{{.inputs.inputs}}",
            "/var/outputs/out",
        ],
    )

    @task
    def flyte_file_task() -> FlyteFile:
        working_dir = flytekit.current_context().working_directory
        write_file = os.path.join(working_dir, "flyte_file.txt")
        with open(write_file, "w") as file:
            file.write("This is flyte_file.txt file.")
        return FlyteFile(path=write_file)

    @workflow
    def flyte_file_io_wf() -> FlyteFile:
        ff = flyte_file_task()
        return flyte_file_io(inputs=ff)

    with pytest.raises(
            AssertionError,
            match=(
                    r"FlyteFile and FlyteDirectory commands should not use the template syntax like this: \{\{\.inputs\.infile\}\}\n"
                    r"Please use a path-like syntax, such as: /var/inputs/infile.\n"
                    r"This requirement is due to how Flyte Propeller processes template syntax inputs."
            )
    ):
        flyte_file_io_wf()

@pytest.mark.skipif(
    sys.platform in ["darwin", "win32"],
    reason="Skip if running on windows or macos due to CI Docker environment setup failure",
)
def test_flytedir_wf():
    client = docker.from_env()
    path_to_dockerfile = "tests/flytekit/unit/core/"
    dockerfile_name = "Dockerfile.raw_container"
    client.images.build(path=path_to_dockerfile, dockerfile=dockerfile_name, tag="flytekit:rawcontainer")

    flyte_dir_io = ContainerTask(
        name="flyte_dir_io",
        input_data_dir="/var/inputs",
        output_data_dir="/var/outputs",
        inputs=kwtypes(inputs=FlyteDirectory),
        outputs=kwtypes(out=FlyteDirectory),
        image="flytekit:rawcontainer",
        command=[
            "python",
            "write_flytedir.py",
            "/var/inputs/inputs",
            "/var/outputs/out",
        ],
    )

    @task
    def flyte_dir_task() -> FlyteDirectory:
        working_dir = flytekit.current_context().working_directory
        local_dir = Path(os.path.join(working_dir, "csv_files"))
        local_dir.mkdir(exist_ok=True)
        write_file = os.path.join(local_dir, "flyte_dir.txt")
        with open(write_file, "w") as file:
            file.write("This is for flyte dir.")

        return FlyteDirectory(path=str(local_dir))

    @workflow
    def flyte_dir_io_wf() -> FlyteDirectory:
        fd = flyte_dir_task()
        return flyte_dir_io(inputs=fd)

    flytyedir = flyte_dir_io_wf()
    assert isinstance(flytyedir, FlyteDirectory)

    with open(os.path.join(flytyedir.path, "flyte_dir.txt"), "r") as file:
        content = file.read()

    assert content == "This is for flyte dir."


@pytest.mark.skipif(
    sys.platform in ["darwin", "win32"],
    reason="Skip if running on windows or macos due to CI Docker environment setup failure",
)
def test_flytedir_wrong_syntax():
    client = docker.from_env()
    path_to_dockerfile = "tests/flytekit/unit/core/"
    dockerfile_name = "Dockerfile.raw_container"
    client.images.build(path=path_to_dockerfile, dockerfile=dockerfile_name, tag="flytekit:rawcontainer")

    flyte_dir_io = ContainerTask(
        name="flyte_dir_io",
        input_data_dir="/var/inputs",
        output_data_dir="/var/outputs",
        inputs=kwtypes(inputs=FlyteDirectory),
        outputs=kwtypes(out=FlyteDirectory),
        image="flytekit:rawcontainer",
        command=[
            "python",
            "write_flytedir.py",
            "{{.inputs.inputs}}",
            "/var/outputs/out",
        ],
    )

    @task
    def flyte_dir_task() -> FlyteDirectory:
        working_dir = flytekit.current_context().working_directory
        local_dir = Path(os.path.join(working_dir, "csv_files"))
        local_dir.mkdir(exist_ok=True)
        write_file = os.path.join(local_dir, "flyte_dir.txt")
        with open(write_file, "w") as file:
            file.write("This is for flyte dir.")

        return FlyteDirectory(path=str(local_dir))

    @workflow
    def flyte_dir_io_wf() -> FlyteDirectory:
        fd = flyte_dir_task()
        return flyte_dir_io(inputs=fd)

    with pytest.raises(
            AssertionError,
            match=(
                    r"FlyteFile and FlyteDirectory commands should not use the template syntax like this: \{\{\.inputs\.infile\}\}\n"
                    r"Please use a path-like syntax, such as: /var/inputs/infile.\n"
                    r"This requirement is due to how Flyte Propeller processes template syntax inputs."
            )
    ):
        flyte_dir_io_wf()


@pytest.mark.skipif(
    sys.platform in ["darwin", "win32"],
    reason="Skip if running on windows or macos due to CI Docker environment setup failure",
)
def test_primitive_types_wf():
    client = docker.from_env()
    path_to_dockerfile = "tests/flytekit/unit/core/"
    dockerfile_name = "Dockerfile.raw_container"
    client.images.build(path=path_to_dockerfile, dockerfile=dockerfile_name, tag="flytekit:rawcontainer")

    python_return_same_values = ContainerTask(
        name="python_return_same_values",
        input_data_dir="/var/inputs",
        output_data_dir="/var/outputs",
        inputs=kwtypes(a=int, b=bool, c=float, d=str, e=datetime.datetime, f=datetime.timedelta),
        outputs=kwtypes(a=int, b=bool, c=float, d=str, e=datetime.datetime, f=datetime.timedelta),
        image="flytekit:rawcontainer",
        command=[
            "python",
            "return_same_value.py",
            "{{.inputs.a}}",
            "{{.inputs.b}}",
            "{{.inputs.c}}",
            "{{.inputs.d}}",
            "{{.inputs.e}}",
            "{{.inputs.f}}",
            "/var/outputs",
        ],
    )

    @workflow
    def primitive_types_io_wf(
        a: int, b: bool, c: float, d: str, e: datetime.datetime, f: datetime.timedelta
    ) -> Tuple[int, bool, float, str, datetime.datetime, datetime.timedelta]:
        return python_return_same_values(a=a, b=b, c=c, d=d, e=e, f=f)

    now = datetime.datetime.now(tz=datetime.timezone.utc)
    a, b, c, d, e, f = primitive_types_io_wf(
        a=0,
        b=False,
        c=3.0,
        d="hello",
        e=now,
        f=datetime.timedelta(days=1, hours=3, minutes=2, seconds=3, microseconds=5),
    )

    assert a == 0
    assert b is False
    assert c == 3.0
    assert d == "hello"
    assert e == now
    assert f == datetime.timedelta(days=1, hours=3, minutes=2, seconds=3, microseconds=5)


@pytest.mark.skipif(
    sys.platform in ["darwin", "win32"],
    reason="Skip if running on windows or macos due to CI Docker environment setup failure",
)
def test_input_output_dir_manipulation():
    client = docker.from_env()
    path_to_dockerfile = "tests/flytekit/unit/core/"
    dockerfile_name = "Dockerfile.raw_container"
    client.images.build(path=path_to_dockerfile, dockerfile=dockerfile_name, tag="flytekit:rawcontainer")

    python_return_same_values = ContainerTask(
        name="python_return_same_values",
        input_data_dir="/inputs",
        output_data_dir="/outputs",
        inputs=kwtypes(a=int, b=bool, c=float, d=str, e=datetime.datetime, f=datetime.timedelta),
        outputs=kwtypes(a=int, b=bool, c=float, d=str, e=datetime.datetime, f=datetime.timedelta),
        image="flytekit:rawcontainer",
        command=[
            "python",
            "return_same_value.py",
            "{{.inputs.a}}",
            "{{.inputs.b}}",
            "{{.inputs.c}}",
            "{{.inputs.d}}",
            "{{.inputs.e}}",
            "{{.inputs.f}}",
            "/outputs",
        ],
    )

    @workflow
    def primitive_types_io_wf(
        a: int, b: bool, c: float, d: str, e: datetime.datetime, f: datetime.timedelta
    ) -> Tuple[int, bool, float, str, datetime.datetime, datetime.timedelta]:
        return python_return_same_values(a=a, b=b, c=c, d=d, e=e, f=f)

    now = datetime.datetime.now(tz=datetime.timezone.utc)
    a, b, c, d, e, f = primitive_types_io_wf(
        a=0,
        b=False,
        c=3.0,
        d="hello",
        e=now,
        f=datetime.timedelta(days=1, hours=3, minutes=2, seconds=3, microseconds=5),
    )

    assert a == 0
    assert b is False
    assert c == 3.0
    assert d == "hello"
    assert e == now
    assert f == datetime.timedelta(days=1, hours=3, minutes=2, seconds=3, microseconds=5)
