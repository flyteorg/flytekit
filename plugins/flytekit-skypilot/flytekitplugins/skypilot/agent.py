from typing import Optional, List, Dict
from dataclasses import dataclass

import sky.check
import sky.clouds.cloud_registry
import sky.core
import sky.resources
from flytekit.models.literals import LiteralMap
from flytekit import logger
from flytekit.models.task import TaskTemplate
from flytekit.extend.backend.base_agent import AsyncAgentBase, AgentRegistry, Resource, ResourceMeta
from flytekitplugins.skypilot.utils import skypilot_status_to_flyte_phase
from flytekitplugins.skypilot.cloud_registry import CloudRegistry, CloudCredentialError, CloudNotInstalledError
from kubernetes import client as k8s_client, config as k8s_config

from flytekit import FlyteContextManager
import sky
import sky.cli as sky_cli
import shlex

TASK_TYPE = "skypilot"


REGISTRY_CONFIG = {
    "name": "http-5000",
    "node_port": 30000,
    "service_name": "flyte-sandbox-docker-registry",
}

@dataclass
class SkyPilotMetadata(ResourceMeta):
    """
    This is the metadata for the job.
    """
    job_id: str
    cluster_name: str
    
def replace_local_registry(container_image: Optional[str]) -> Optional[str]:
    
    local_registry = f"localhost:{REGISTRY_CONFIG['node_port']}"
    if container_image is None or local_registry not in container_image:
        return container_image
    k8s_config.load_kube_config()
    v1 = k8s_client.CoreV1Api()
    services = v1.list_namespaced_service("flyte")
    for service in services.items:
        if service.metadata.name == REGISTRY_CONFIG["service_name"]:
            for port in service.spec.ports:
                if port.name == REGISTRY_CONFIG["name"]:
                    return container_image.replace(local_registry, f"{REGISTRY_CONFIG['service_name']}:{port.port}")
                    
    return container_image


def parse_sky_resources(task_template: TaskTemplate) -> List[sky.Resources]:
    resources: List[Dict[str, str]] = task_template.custom["resource_config"]
    container_image: str = task_template.container.image
    new_resource_list = []
    # return [sky.resources.Resources(image_id=f"docker:localhost:30000/flytekit:skypilot")]
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
        print(resource)
        new_resource = sky.resources.Resources(**resource)
        new_resource_list.append(new_resource)
        
    if not new_resource_list:
        new_resource_list.append(sky.resources.Resources(image_id=f"docker:{container_image}"))
    return new_resource_list
    

class SkyPilotAgent(AsyncAgentBase):
    def __init__(self):
        super().__init__(task_type_name=TASK_TYPE, metadata_type=SkyPilotMetadata)
        # import pdb
        # pdb.set_trace()

    def setup_cloud_credential(self, task: sky.Task):
        cloud_provider_types = CloudRegistry.list_clouds()
        installed_cloud_providers: List[str] = []
        cred_not_provided_clouds: List[str] = []
        for _cloud_type in cloud_provider_types:
            try:
                provider = _cloud_type()
                try:
                    provider.setup_cloud_credential()
                    installed_cloud_providers.append(provider._CLOUD_TYPE)
                except CloudCredentialError as e:
                    cred_not_provided_clouds.append(provider._CLOUD_TYPE)
                    continue

            except CloudNotInstalledError as e:
                continue
                
        logger.warning(f"Installed cloud providers: {installed_cloud_providers}")
        sky.check.check()
        return

    def create(
        self,
        task_template: TaskTemplate,
        inputs: Optional[LiteralMap] = None,
        **kwargs,
    ) -> SkyPilotMetadata:
        ctx = FlyteContextManager.current_context()
        print(f"Creating... Fucking SkyPilot {task_template.container.args} | {task_template.container.image}")
        cluster_name: str = task_template.custom["cluster_name"]
        # sky_resources: List[Dict[str, str]] = task_template.custom["resource_config"]
        sky_resources = parse_sky_resources(task_template)
        # resource = task_template.container.resources
        local_env_prefix = "\n".join([f"export {k}='{v}'" for k, v in task_template.custom["local_envs"].items()])
        raw_task_command = shlex.join(task_template.container.args)
        full_task_command = "\n".join([local_env_prefix, raw_task_command]).strip()
        task = sky.Task(run=full_task_command)
        task.set_resources(sky_resources).set_file_mounts(task_template.custom["file_mounts"])
        backend = sky.backends.CloudVmRayBackend()
        
        self.setup_cloud_credential(task)
        if task_template.custom["prompt_cloud"]:
            job_id, _ = sky_cli._launch_with_confirm(
                task=task,
                cluster=cluster_name,
                backend=backend,
                dryrun=False,
                detach_run=False
            )
        else:
            import pdb
            # pdb.set_trace()
            job_id, _ = sky.launch(
                task=task, 
                cluster_name=cluster_name, 
                backend=backend, 
            )
        # sky.check()
        # print("finished creating")
        return SkyPilotMetadata(job_id=job_id, cluster_name=cluster_name)

    def get(self, resource_meta: SkyPilotMetadata, **kwargs) -> Resource:
        # phase, outputs = get_skypilot_job_status(...)
        from flyteidl.core.execution_pb2 import TaskExecution

        phase = TaskExecution.Phase.FAILED
        job_list = sky.core.queue(resource_meta.cluster_name)
        import pdb
        # pdb.set_trace()
        for job in job_list:
            if job["job_id"] == resource_meta.job_id:
                
                phase = skypilot_status_to_flyte_phase(job["status"])
                print(phase, job)
                break

        return Resource(phase=phase)

    def delete(self, resource_meta: SkyPilotMetadata, **kwargs):
        import pdb
        # pdb.set_trace()
        sky.stop(resource_meta.cluster_name)

# To register the skypilot agent
AgentRegistry.register(SkyPilotAgent())