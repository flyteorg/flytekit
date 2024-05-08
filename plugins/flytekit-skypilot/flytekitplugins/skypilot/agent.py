from typing import Optional, List, Dict, Any, Tuple
from dataclasses import dataclass
import asyncio
import sky.check
import sky.clouds.cloud_registry
import sky.core
import sky.exceptions
import sky.resources
from flytekit.models.literals import LiteralMap
from flytekit import logger
from flytekit.models.task import TaskTemplate
import flytekit
from flytekit.extend.backend.base_agent import AsyncAgentBase, AgentRegistry, Resource, ResourceMeta
from flytekitplugins.skypilot.utils import skypilot_status_to_flyte_phase,\
    execute_task_cmd, map_execute_task_cmd, fast_execute_task_cmd
from flytekit.core.data_persistence import FileAccessProvider
from flytekitplugins.skypilot.cloud_registry import CloudRegistry, CloudCredentialError, CloudNotInstalledError
from kubernetes import client as k8s_client, config as k8s_config
import enum
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

class SkyFutureStatus(enum.Enum):
    """
    This is the status of the job.
    """
    INITIALIZING = "INITIALIZING"
    RUNNING = "RUNNING"

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


def setup_cloud_credential():
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
        logger.info(resource)
        new_resource = sky.resources.Resources(**resource)
        new_resource_list.append(new_resource)
        
    if not new_resource_list:
        new_resource_list.append(sky.resources.Resources(image_id=f"docker:{container_image}"))
    return new_resource_list
    


class SkyTaskFuture(object):
    _job_id: int = -1  # not executed yet
    _job_status: SkyFutureStatus = SkyFutureStatus.INITIALIZING
    _task_kwargs: Dict[str, Any] = None
    _coro: asyncio.Task = None
    def __init__(self, task_template):
        self._task_kwargs = task_template

    def launch(self):
        cluster_name: str = self.task_template.custom["cluster_name"]
        # sky_resources: List[Dict[str, str]] = task_template.custom["resource_config"]
        sky_resources = parse_sky_resources(self.task_template)
        local_env_prefix = "\n".join([f"export {k}='{v}'" for k, v in self.task_template.custom["local_config"]["local_envs"].items()])
        raw_task_command = shlex.join(self.task_template.container.args)
        full_task_command = "\n".join([local_env_prefix, raw_task_command]).strip()
        task = sky.Task(run=full_task_command)
        task.set_resources(sky_resources).set_file_mounts(self.task_template.custom["file_mounts"])
        backend = sky.backends.CloudVmRayBackend()
    
        if self.task_template.custom["prompt_cloud"]:
            job_id, _ = sky_cli._launch_with_confirm(
                task=task,
                cluster=cluster_name,
                backend=backend,
                dryrun=False,
                detach_run=False
            )
        else:
            job_id, _ = sky.launch(
                task=task, 
                cluster_name=cluster_name, 
                backend=backend, 
            )
        return job_id
    
    @property
    def task_template(self):
        return self._task_kwargs
    
    def start(self):
        
        self._coro = asyncio.create_task(asyncio.to_thread(self.launch))
        return
    
    def cancel(self):
        if self._coro is not None:
            self._coro.cancel()
        return
    
    def done(self) -> Tuple[int, SkyFutureStatus]:
        logger.warning(f"SkyTaskFuture.done(): {self._coro.done()}")
        if self._job_status == SkyFutureStatus.INITIALIZING and self._coro.done():
            self._job_id = self._coro.result()
            self._job_status = SkyFutureStatus.RUNNING

        return self._job_id, self._job_status

class SkyTaskTracker(object):
    _job_count = 0
    _JOB_RESIGTRY: Dict[int, SkyTaskFuture] = {}
    _JOB_ID_MAP = {}
    
    @staticmethod
    def register_sky_task(**kwargs):
        SkyTaskTracker._job_count += 1
        new_task = SkyTaskFuture(**kwargs)
        new_task.start()
        SkyTaskTracker._JOB_RESIGTRY[SkyTaskTracker._job_count] = new_task
        return SkyTaskTracker._job_count

    @staticmethod
    def get(job_id: int) -> Tuple[int, SkyFutureStatus]:
        return SkyTaskTracker._JOB_RESIGTRY[job_id].done()

class SkyPilotAgent(AsyncAgentBase):
    _args = None
    _path_mapping = None
    _cloud_checked: bool = False
    def __init__(self):
        super().__init__(task_type_name=TASK_TYPE, metadata_type=SkyPilotMetadata)
        # import pdb
        # pdb.set_trace()
        self._args = {}
        self._path_mapping = {}
        self._launch_map = {}

    
    def execute_cmd_to_path(self, cmd: List[str]) -> str:
        assert len(cmd) > 0
        for cmd_entrypoint in [fast_execute_task_cmd, execute_task_cmd, map_execute_task_cmd]:
            if cmd_entrypoint.name == cmd[0]:
                ctx = cmd_entrypoint.make_context(info_name="", args=cmd[1:])
                args = ctx.params
                self._args.update(args)
                break
            
        file_provider = FileAccessProvider(local_sandbox_dir="/tmp", raw_output_prefix=self._args["output_prefix"])
        for arg in self._args:
            if arg == "inputs":
                inputs = self._args[arg]
                if file_provider.is_remote(inputs):
                    rand_file = file_provider.get_random_local_path()
                    file_provider.get_data(inputs, rand_file)
                    self._path_mapping["inputs"] = rand_file
            elif arg == "output_prefix":
                output_prefix = self._args[arg]
                if file_provider.is_remote(output_prefix):
                    rand_dir = file_provider.get_random_local_directory()
                    self._path_mapping["output_prefix"] = rand_file
            elif arg == "outputs":
                outputs = self._args[arg]
                if isinstance(outputs, str):
                    pass
                
    async def create(
        self,
        task_template: TaskTemplate,
        inputs: Optional[LiteralMap] = None,
        **kwargs,
    ) -> SkyPilotMetadata:
        if not self._cloud_checked:
            setup_cloud_credential()
            self._cloud_checked = True

        # ctx = FlyteContextManager.current_context()
        logger.warning(f"Creating... Fucking SkyPilot {task_template.container.args} | {task_template.container.image}")
        job_id = SkyTaskTracker.register_sky_task(task_template=task_template)
        import pdb
        # pdb.set_trace()
        logger.warning(f"Created SkyPilot {job_id}")
        # await SkyTaskTracker._JOB_RESIGTRY[job_id]._coro
        return SkyPilotMetadata(job_id=job_id, cluster_name=task_template.custom["cluster_name"])
        

    async def get(self, resource_meta: SkyPilotMetadata, **kwargs) -> Resource:
        from flyteidl.core.execution_pb2 import TaskExecution
        phase = TaskExecution.Phase.FAILED
        sky_task_id, sky_task_status = SkyTaskTracker.get(resource_meta.job_id)
        if sky_task_status == SkyFutureStatus.INITIALIZING:
            return Resource(phase=TaskExecution.Phase.INITIALIZING)
        try:
            job_list = sky.core.queue(resource_meta.cluster_name)
        except sky.exceptions.ClusterNotUpError as e:
            return Resource(phase=TaskExecution.Phase.QUEUED)
        for job in job_list:
            if job["job_id"] == resource_meta.job_id:
                
                phase = skypilot_status_to_flyte_phase(job["status"])
                logger.info(f"Phase: {phase}, {job}")
                break

        return Resource(phase=phase)

    async def delete(self, resource_meta: SkyPilotMetadata, **kwargs):
        import pdb
        # pdb.set_trace()
        sky.stop(resource_meta.cluster_name)

# To register the skypilot agent
AgentRegistry.register(SkyPilotAgent())



# def create():
#     cluster_name: str = task_template.custom["cluster_name"]
#         # sky_resources: List[Dict[str, str]] = task_template.custom["resource_config"]
#         sky_resources = parse_sky_resources(task_template)
#         import pdb
#         # pdb.set_trace()
#         # ctx = flytekit.current_context()
#         # secret_val = ctx.secrets.get("aws_configure", "aws_access_key_id")
        
#         # resource = task_template.container.resources
#         local_env_prefix = "\n".join([f"export {k}='{v}'" for k, v in task_template.custom["local_config"]["local_envs"].items()])
#         raw_task_command = shlex.join(task_template.container.args)
#         full_task_command = "\n".join([local_env_prefix, raw_task_command]).strip()
#         task = sky.Task(run=full_task_command)
#         task.set_resources(sky_resources).set_file_mounts(task_template.custom["file_mounts"])
#         backend = sky.backends.CloudVmRayBackend()
        
#         self.setup_cloud_credential(task)
#         if task_template.custom["prompt_cloud"]:
#             job_id, _ = sky_cli._launch_with_confirm(
#                 task=task,
#                 cluster=cluster_name,
#                 backend=backend,
#                 dryrun=False,
#                 detach_run=False
#             )
#         else:
#             import pdb
#             # pdb.set_trace()
#             job_id, _ = sky.launch(
#                 task=task, 
#                 cluster_name=cluster_name, 
#                 backend=backend, 
#             )
#         # sky.check()
#         # print("finished creating")
#         return SkyPilotMetadata(job_id=job_id, cluster_name=cluster_name)
