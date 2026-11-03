import os.path
from unittest import mock
from flytekit.core import context_manager
import flytekit
from flytekit import StructuredDataset, StructuredDatasetTransformerEngine, task, ImageSpec
from flytekit.configuration import Image, ImageConfig, SerializationSettings, FastSerializationSettings, DefaultImages
from flytekit.core.context_manager import ExecutionParameters, FlyteContextManager, ExecutionState
from flytekitplugins.slurm import SlurmFunctionConfig


def test_slurm_task():
    script_file = """#!/bin/bash -i

    echo Run function with sbatch...

    # Run the user-defined task function
    {task.fn}
    """

    @task(
    # container_image=image,
    task_config=SlurmFunctionConfig(
        ssh_config={
            "host": "your-slurm-host",
            "username": "ubuntu",
        },
        sbatch_config={
            "partition": "debug",
            "job-name": "tiny-slurm",
            "output": "/home/ubuntu/fn_task.log"
        },
        script=script_file
        )
    )
    def plus_one(x: int) -> int:
        return x + 1

    assert plus_one.task_config is not None
    assert plus_one.task_config.ssh_config == {"host": "your-slurm-host", "username": "ubuntu"}
    assert plus_one.task_config.sbatch_config == {"partition": "debug", "job-name": "tiny-slurm", "output": "/home/ubuntu/fn_task.log"}
    assert plus_one.task_config.script == script_file

    default_img = Image(name="default", fqn="test", tag="tag")
    settings = SerializationSettings(
        project="project",
        domain="domain",
        version="version",
        env={"FOO": "baz"},
        image_config=ImageConfig(default_image=default_img, images=[default_img]),
    )

    retrieved_settings = plus_one.get_custom(settings)
    assert retrieved_settings["ssh_config"] == {"host": "your-slurm-host", "username": "ubuntu"}
    assert retrieved_settings["sbatch_config"] == {"partition": "debug", "job-name": "tiny-slurm", "output": "/home/ubuntu/fn_task.log"}
    assert retrieved_settings["script"] == script_file
