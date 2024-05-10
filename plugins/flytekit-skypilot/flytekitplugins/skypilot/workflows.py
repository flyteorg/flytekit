from typing import Any, Dict, Optional, Union, Callable, List, Set
from dataclasses import dataclass, asdict
import os
from typing import Any, Callable, Dict, Optional, Union, cast

from google.protobuf.json_format import MessageToDict

from flytekit import FlyteContextManager, PythonFunctionTask, lazy_module, logger, Workflow, workflow, task
from flytekit.configuration import DefaultImages, SerializationSettings
from flytekit.core.base_task import PythonTask
from flytekit.core.context_manager import ExecutionParameters
from flytekit.core.python_auto_container import get_registerable_container_image
from flytekit.extend import ExecutionState, TaskPlugins
from flytekit.extend.backend.base_agent import AsyncAgentExecutorMixin
from flytekit.image_spec import ImageSpec
from flytekit.types.file import FlyteFile
import sky
from sky import resources as resources_lib
from flytekit.models.literals import LiteralMap
from flytekitplugins.skypilot import SkyPilot
from flytekitplugins.skypilot.agent import parse_sky_resources


def sky_config_to_resource(sky_config: SkyPilot, container_image: str=None) -> resources_lib.Resource:
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


@workflow
def create_cluster(task_config: SkyPilot) -> FlyteFile:
    cluster_name: str = task_config.cluster_name
    sky_resources = sky_config_to_resource(task_config)
    task = sky.Task()
    task.set_resources(sky_resources).set_file_mounts(task_config.file_mounts)
    backend = sky.backends.CloudVmRayBackend()
    job_id, _ = sky.launch(
        task=task, 
        cluster_name=cluster_name, 
        backend=backend, 
    )
    
    return FlyteFile(sky._DB_PATH)



# class SkyPilotContainerTask

class SkyPilotTask(object, Workflow):
    def __init__(self, name: str, task_config: SkyPilot) -> None:
        Workflow.__init__(self, name=name)
        self._task_config = task_config



wf = SkyPilotTask(name="SkyPilotTask", task_config=SkyPilot(cluster_name="test", resource_config=[{"cloud": "kubernetes"}]))
