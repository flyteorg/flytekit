import logging
from flytekit import ContainerTask, kwtypes, workflow, task
from flytekit.types.file import FlyteFile
from flytekit.types.directory import FlyteDirectory


logger = logging.getLogger(__file__)

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

flyte_dir_io = ContainerTask(
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
    with open("./a.txt", "w") as file:
        file.write("This is a.txt file.")
    return FlyteFile(path="./a.txt")


@workflow
def flyte_file_io_wf() -> FlyteFile:
    ff = flyte_file_task()
    return flyte_file_io(inputs=ff)


@task
def flyte_dir_task() -> FlyteDirectory:
    from pathlib import Path
    import flytekit
    import os

    working_dir = flytekit.current_context().working_directory
    local_dir = Path(os.path.join(working_dir, "csv_files"))
    local_dir.mkdir(exist_ok=True)
    write_file = local_dir / "a.txt"
    with open(write_file, "w") as file:
        file.write("This is for flyte dir.")

    return FlyteDirectory(path=str(local_dir))


@workflow
def flyte_dir_io_wf() -> FlyteDirectory:
    fd = flyte_dir_task()
    return flyte_dir_io(inputs=fd)


if __name__ == "__main__":
    print(flyte_dir_io_wf())
    print(flyte_file_io_wf())
