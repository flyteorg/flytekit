import os
from typing import Callable, Dict, List

import sky
from flytekitplugins.skypilot.task import SkyPilot
from flytekitplugins.skypilot.utils import LocalPathSetting, setup_cloud_credential
from sky import resources as resources_lib

import flytekit
from flytekit import FlyteContextManager, PythonFunctionTask, logger, task
from flytekit.types.file import FlyteFile


def sky_config_to_resource(sky_config: SkyPilot, container_image: str = None) -> resources_lib.Resources:
    resources: List[Dict[str, str]] = sky_config.resource_config
    new_resource_list = []
    for resource in resources:
        disk_tier = resource.pop("disk_tier", None)
        if disk_tier is not None:
            resource["disk_tier"] = sky.resources.resources_utils.DiskTier(disk_tier.lower())
        cloud = resource.pop("cloud", None)
        resource["cloud"] = sky.clouds.cloud_registry.CLOUD_REGISTRY.from_str(cloud)
        image = resource.pop("image_id", None)
        if image is None:
            image = f"docker:{container_image}"
            # if cloud != "kubernetes":  # remote cluster
            # image = replace_local_registry(image)
        resource["image_id"] = image
        logger.info(resource)
        new_resource = sky.resources.Resources(**resource)
        new_resource_list.append(new_resource)

    if not new_resource_list:
        new_resource_list.append(sky.resources.Resources(image_id=f"docker:{container_image}"))
    return new_resource_list


def empty_task() -> tuple[str, str]:
    return sky.utils.common_utils.get_user_hash(), sky.jobs.utils.JOB_CONTROLLER_NAME


def clean_up(user_hash: str, controller_name: str) -> None:
    sky.utils.common_utils.get_user_hash = lambda: user_hash
    sky.jobs.utils.JOB_CONTROLLER_NAME = controller_name
    sky.down(controller_name)


def create_cluster(user_hash: str, cluster_name: str) -> tuple[FlyteFile, FlyteFile, str]:
    # get job controller file
    config_url = os.environ.get("SKYPILOT_CONFIG_URL", None)
    if config_url:
        download_and_set_sky_config(config_url)

    setup_cloud_credential()
    sample_task_config = {"resources": {"cpu": "1", "memory": "1", "use_spot": True}}
    sample_task = sky.Task.from_yaml_config(sample_task_config)
    sky.utils.common_utils.get_user_hash = lambda: user_hash
    sky.jobs.utils.JOB_CONTROLLER_NAME = cluster_name
    sky.jobs.launch(sample_task)
    path_setting = LocalPathSetting(
        file_access=FlyteContextManager.current_context().file_access,
        execution_id=flytekit.current_context().task_id.version,
    )
    path_setting.zip_sky_info()
    return FlyteFile(path_setting.home_sky_zip), FlyteFile(path_setting.sky_key_zip), cluster_name


# TODO: Trying to separate tasks, but I don't think this would be any better given skypilot's slow api.
def load_sky_config():
    secret_manager = flytekit.current_context().secrets
    try:
        config_url = secret_manager.get(
            group="sky",
            key="config",
        )
    except ValueError:
        logger.warning("sky config not set, will use default controller setting")
        return

    download_and_set_sky_config(config_url)


def download_and_set_sky_config(config_url: str):
    ctx = FlyteContextManager.current_context()
    file_access = ctx.file_access
    file_access.get_data(config_url, os.path.expanduser(sky.skypilot_config.CONFIG_PATH))
    sky.skypilot_config._try_load_config()


# write a decorator for function, the decorator must be able to take in the task_config and return a new function
def sky_pilot_task(task_config: SkyPilot, **kwargs) -> Callable:
    def wrapper(func: Callable) -> Callable:
        create_cluster_func = task(create_cluster, **kwargs)
        assert isinstance(create_cluster_func, PythonFunctionTask)

    return wrapper
