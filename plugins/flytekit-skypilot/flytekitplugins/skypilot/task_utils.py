import copy
import enum
import hashlib
import json
import shlex
from typing import Any, Dict

import sky

from flytekit.models.task import TaskTemplate


class ContainerRunType(int, enum.Enum):
    RUNTIME = 0  # use container as runtime environment
    APP = 1  # use as docker run {image} {command}


def parse_sky_resources(task_template: TaskTemplate) -> Dict[str, Any]:
    sky_task_config = {}
    resources: Dict[str, Any] = task_template.custom["resource_config"]
    container_image: str = task_template.container.image
    if (
        resources.get("image_id", None) is None
        and task_template.custom["container_run_type"] == ContainerRunType.RUNTIME
    ):
        resources["image_id"] = f"docker:{container_image}"

    sky_task_config.update(
        {
            "resources": resources,
            "file_mounts": task_template.custom["file_mounts"],
        }
    )
    return sky_task_config


class SetupCommand(object):
    docker_pull: str = None
    flytekit_pip: str = None
    full_setup: str = None

    def __init__(self, task_template: TaskTemplate) -> None:
        task_setup = task_template.custom["setup"]
        if task_template.custom["container_run_type"] == ContainerRunType.APP:
            self.docker_pull = f"docker pull {task_template.container.image}"
        else:
            # HACK, change back to normal flytekit
            self.flytekit_pip = ""

        self.full_setup = "\n".join(filter(None, [task_setup, self.docker_pull, self.flytekit_pip])).strip()


class RunCommand(object):
    full_task_command: str = None
    _use_gpu: bool = False

    def __init__(self, task_template: TaskTemplate) -> None:
        raw_task_command = shlex.join(task_template.container.args)
        self.check_resource(task_template)
        if task_template.custom["container_run_type"] == ContainerRunType.RUNTIME:
            python_path_command = f"export PYTHONPATH=$PYTHONPATH:$HOME/{sky.backends.docker_utils.SKY_DOCKER_WORKDIR}"
            self.full_task_command = "\n".join(filter(None, [python_path_command, raw_task_command])).strip()
        else:
            container_entrypoint, container_args = task_template.container.args[0], task_template.container.args[1:]
            docker_run_prefix = f"docker run {'--gpus=all' if self.use_gpu else ''} --entrypoint {container_entrypoint}"
            volume_setups, cloud_cred_envs = [], []
            volume_command = " ".join(volume_setups)
            cloud_cred_env_command = " ".join(cloud_cred_envs)
            self.full_task_command = " ".join(
                [
                    docker_run_prefix,
                    volume_command,
                    cloud_cred_env_command,
                    task_template.container.image,
                    *container_args,
                ]
            )

    @property
    def use_gpu(self) -> bool:
        return self._use_gpu

    def check_resource(self, task_template: TaskTemplate) -> None:
        gpu_config = parse_sky_resources(task_template)
        gpu_task = sky.Task.from_yaml_config(gpu_config)
        for resource in gpu_task.resources:
            if resource.accelerators is not None:
                self._use_gpu = True
                break


def get_sky_task_config(task_template: TaskTemplate) -> Dict[str, Any]:
    sky_task_config = parse_sky_resources(task_template)
    sky_task_config.update(
        {
            # build setup commands
            "setup": SetupCommand(task_template).full_setup,
            # build run commands
            "run": RunCommand(task_template).full_task_command,
        }
    )
    return sky_task_config


def get_cluster_suffix(task_config: Dict, hash_length=16):
    # copy the input dict
    new_config = copy.deepcopy(task_config)
    new_config.pop("run", None)
    # Convert dict to a sorted JSON string
    json_str = json.dumps(new_config, sort_keys=True)

    # Create SHA-256 hash
    hash_obj = hashlib.sha256(json_str.encode())

    # Return first 'hash_length' characters of the hexadecimal digest
    return hash_obj.hexdigest()[:hash_length]
