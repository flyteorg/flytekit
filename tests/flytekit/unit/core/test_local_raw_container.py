from typing import Tuple, List
import sys
import pytest
from flytekit import ContainerTask, kwtypes, workflow, task
from flytekit.types.file import FlyteFile
from flytekit.types.directory import FlyteDirectory
import docker
from pathlib import Path
import flytekit
import os

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

    flytefile = flyte_file_io_wf()
    assert isinstance(flytefile, FlyteFile)

    with open(flytefile.path, 'r') as file:
        content = file.read()
    
    assert content == "This is flyte_file.txt file."
    

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
        # image="futureoutlier/rawcontainer:0320",
        image="flytekit:rawcontainer",
        command=[
            "python",
            "write_flytedir.py",
            "{{.inputs.inputs}}",
            # "/var/inputs/inputs",
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

    with open(os.path.join(flytyedir.path, "flyte_dir.txt"), 'r') as file:
        content = file.read()
    
    assert content == "This is for flyte dir."
