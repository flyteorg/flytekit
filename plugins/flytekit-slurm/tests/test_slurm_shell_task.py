from flytekit.configuration import Image, ImageConfig, SerializationSettings
from flytekitplugins.slurm import SlurmConfig, SlurmScriptConfig, SlurmTask, SlurmShellTask


def test_slurm_shell_task():
    """Test SlurmTask and SlurmShellTask."""
    batch_script_path = "/script/path/on/the/slurm/cluster"
    script = """#!/bin/bash -i

    echo "Run a Flyte SlurmShellTask...\n"
    """

    # SlurmTask
    slurm_task = SlurmTask(
        name="test-slurm-task",
        task_config=SlurmScriptConfig(
            ssh_config={
                "host": "<your-slurm-host>",
                "username": "ubuntu",
            },
            sbatch_config={
                "partition": "debug",
                "job-name": "tiny-slurm",
                "output": "/home/ubuntu/slurm_task.log"
            },
            batch_script_path=batch_script_path
        )
    )

    assert slurm_task.task_config is not None
    assert slurm_task.task_config.ssh_config == {"host": "<your-slurm-host>", "username": "ubuntu"}
    assert slurm_task.task_config.sbatch_config == {"partition": "debug", "job-name": "tiny-slurm", "output": "/home/ubuntu/slurm_task.log"}
    assert slurm_task.task_config.batch_script_path == batch_script_path

    # SlurmShellTask
    slurm_shell_task = SlurmShellTask(
        name="test-slurm-shell-task",
        script=script,
        task_config=SlurmConfig(
            ssh_config={
                "host": "<your-slurm-host>",
                "username": "ubuntu",
            },
            sbatch_config={
                "partition": "debug",
                "job-name": "tiny-slurm",
                "output": "/home/ubuntu/slurm_shell_task.log"
            }
        )
    )

    assert slurm_shell_task.task_config is not None
    assert slurm_shell_task.task_config.ssh_config == {"host": "<your-slurm-host>", "username": "ubuntu"}
    assert slurm_shell_task.task_config.sbatch_config == {"partition": "debug", "job-name": "tiny-slurm", "output": "/home/ubuntu/slurm_shell_task.log"}
    assert slurm_shell_task.script == script

    # Define dummy SerializationSettings
    default_img = Image(name="default", fqn="test", tag="tag")
    settings = SerializationSettings(
        project="project",
        domain="domain",
        version="version",
        env={"FOO": "baz"},
        image_config=ImageConfig(default_image=default_img, images=[default_img])
    )

    custom = slurm_task.get_custom(settings)
    assert custom["ssh_config"] == {"host": "<your-slurm-host>", "username": "ubuntu"}
    assert custom["sbatch_config"] == {"partition": "debug", "job-name": "tiny-slurm", "output": "/home/ubuntu/slurm_task.log"}
    assert custom["batch_script_path"] == batch_script_path
    assert custom["batch_script_args"] is None

    shell_custom = slurm_shell_task.get_custom(settings)
    assert shell_custom["ssh_config"] == {"host": "<your-slurm-host>", "username": "ubuntu"}
    assert shell_custom["sbatch_config"] == {"partition": "debug", "job-name": "tiny-slurm", "output": "/home/ubuntu/slurm_shell_task.log"}
    assert shell_custom["script"] == script
    assert shell_custom["batch_script_args"] is None
