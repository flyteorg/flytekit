from typing import Optional, List, Dict, Any, Tuple
from dataclasses import dataclass
import asyncio
import sky.check
import sky.clouds.cloud_registry
import sky.core
import sky.exceptions
import sky.resources
import os
from flytekit.models.literals import LiteralMap
from flytekit import logger
from flytekit.models.task import TaskTemplate
import flytekit
from flytekit.extend.backend.base_agent import AsyncAgentBase, AgentRegistry, Resource, ResourceMeta
from flytekitplugins.skypilot.utils import skypilot_status_to_flyte_phase, execute_cmd_to_path
from flytekit.core.data_persistence import FileAccessProvider
from flytekit.core import utils
from flyteidl.core import literals_pb2 as _literals_pb2
from flytekitplugins.skypilot.cloud_registry import CloudRegistry, CloudCredentialError, CloudNotInstalledError
from kubernetes import client as k8s_client, config as k8s_config
import contextvars
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

_db_dir_ctx_Var = contextvars.ContextVar('original_db', default=[os.path.expanduser("~/.sky")])
_ssh_ctx_Var = contextvars.ContextVar('original_ssh', default=[os.path.expanduser("~/.ssh")])
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
    job_name: str
    cluster_name: str
    output_blob_prefix: str
    sky_zip_prefix: str
    
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
    
    
@dataclass
class RemotePathSetting:
    file_access: FileAccessProvider
    execution_id: str
    remote_sky_dir: str = None
    remote_sky_zip: str = None
    remote_key_zip: str = None
    def __post_init__(self):
        self.remote_sky_dir = self.file_access.join(self.file_access.raw_output_prefix, ".skys")
        self.remote_sky_zip = self.file_access.join(self.remote_sky_dir, "home_sky.tar.gz")
        self.remote_key_zip = self.file_access.join(self.remote_sky_dir, "sky_key.tar.gz")

@dataclass
class LocalPathSetting:
    file_access: FileAccessProvider
    execution_id: str
    local_sky_prefix: str = None
    home_sky_zip: str = None
    sky_key_zip: str = None
    home_sky_dir: str = None
    home_key_dir: str = None
    def __post_init__(self):
        self.local_sky_prefix = os.path.join(self.file_access.local_sandbox_dir, self.execution_id, ".skys")
        self.home_sky_zip = os.path.join(self.local_sky_prefix, "home_sky")
        self.sky_key_zip = os.path.join(self.local_sky_prefix, "sky_key.tar.gz")
        self.home_sky_dir = os.path.join(self.local_sky_prefix, ".sky")
        self.home_key_dir = os.path.join(self.local_sky_prefix, ".ssh")
    
@dataclass
class SkyPathSetting:
    args: Dict[str, str] = None
    execution_id: str = None
    local_path_setting: LocalPathSetting = None
    remote_path_setting: RemotePathSetting = None
    file_access: FileAccessProvider = None
    
    def __post_init__(self):
        self.file_access = file_provider = FileAccessProvider(local_sandbox_dir="/tmp", raw_output_prefix=self.args["raw_output_data_prefix"])
        self.execution_id = self.args["raw_output_data_prefix"].split(file_provider.get_filesystem().sep)[-1].replace("-", "_")
        self.local_path_setting = LocalPathSetting(file_access=file_provider, execution_id=self.execution_id)
        self.remote_path_setting = RemotePathSetting(file_access=file_provider, execution_id=self.execution_id)
        
    def remote_exists(self):
        return self.file_access.exists(self.remote_path_setting.remote_sky_zip)\
            or self.file_access.exists(self.remote_path_setting.remote_key_zip)
        
    async def zip_and_upload(self, zip_every_n_seconds: int = 5):
        # compress ~/.sky to home_sky.tar.gz
        # put ~/.ssh/sky-key* to sky_key.tar.gz
        logger.warning(self.local_path_setting)
        logger.warning(self.remote_path_setting)
        import tarfile
        import shutil
        while True:
            sky_zip = shutil.make_archive(self.local_path_setting.home_sky_zip, 'gztar', os.path.expanduser("~/.sky"))
            self.file_access.put_data(sky_zip, self.remote_path_setting.remote_sky_zip)
            # tar ~/.ssh/sky-key* to sky_key.tar
            local_key_dir = os.path.expanduser("~/.ssh")
            archived_keys = [file for file in os.listdir(local_key_dir) if file.startswith("sky-key")]
            with tarfile.open(self.local_path_setting.sky_key_zip, 'w:gz') as key_tar:
                for key_file in archived_keys:
                    key_tar.add(os.path.join(local_key_dir, key_file), arcname=os.path.basename(key_file))
            
            self.file_access.put_data(self.local_path_setting.sky_key_zip, self.remote_path_setting.remote_key_zip)
            await asyncio.sleep(zip_every_n_seconds)


    def download_and_unzip(self):
        import tarfile
        import shutil
        self.file_access.get_data(self.remote_path_setting.remote_sky_zip, self.local_path_setting.home_sky_zip)
        self.file_access.get_data(self.remote_path_setting.remote_key_zip, self.local_path_setting.sky_key_zip)
        shutil.unpack_archive(self.local_path_setting.home_sky_zip, self.local_path_setting.home_sky_dir)
        with tarfile.open(self.local_path_setting.sky_key_zip, 'r:gz') as key_tar:
            key_tar.extractall(self.local_path_setting.home_key_dir)
        return


class SkyTaskFuture(object):
    _job_id: int = -1  # not executed yet
    _job_status: SkyFutureStatus = SkyFutureStatus.INITIALIZING
    _task_kwargs: Dict[str, Any] = None
    _launch_coro: asyncio.Task = None
    _zip_coro: asyncio.Task = None
    _sky_path_setting: SkyPathSetting = None
    _task_name: str = None
    def __init__(self, task_template: TaskTemplate):
        self._task_kwargs = task_template
        args = execute_cmd_to_path(task_template.container.args)
        self._sky_path_setting = SkyPathSetting(args=args)
        task_suffix = self._sky_path_setting.execution_id
        self._task_name = f'{self.task_template.custom["task_name"]}.{task_suffix}'

    def launch(self):
        cluster_name: str = self.task_template.custom["cluster_name"]
        # sky_resources: List[Dict[str, str]] = task_template.custom["resource_config"]
        sky_resources = parse_sky_resources(self.task_template)
        local_env_prefix = "\n".join([f"export {k}='{v}'" for k, v in self.task_template.custom["local_config"]["local_envs"].items()])
        raw_task_command = shlex.join(self.task_template.container.args)
        full_task_command = "\n".join([local_env_prefix, raw_task_command]).strip()
        
        task = sky.Task(run=full_task_command, name=self._task_name)
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
    def sky_path_setting(self) -> SkyPathSetting:
        return self._sky_path_setting

    @property
    def task_template(self) -> TaskTemplate:
        return self._task_kwargs
    
    @property
    def job_id(self) -> int:
        return self._job_id
    
    @property
    def task_name(self) -> str:
        return self._task_name

    def start(self) -> bool:
        '''
        Before starting, check if the remote ~/.sky and ~/.ssh/sky-key* exists.
        This is to avoid repeated task creation, but the check and upload isn't atomic.
        '''
        if not self.sky_path_setting.remote_exists():
            self._launch_coro = asyncio.create_task(asyncio.to_thread(self.launch))
            self._zip_coro = asyncio.create_task(self.sky_path_setting.zip_and_upload())
            return True
        
        return False
    
    def cancel(self):
        if self._launch_coro is not None:
            self._launch_coro.cancel()
        if self._zip_coro is not None:
            self._zip_coro.cancel()
        return
    
    
    # deprecated
    def done(self) -> Tuple[int, SkyFutureStatus]:
        logger.warning(f"SkyTaskFuture.done(): {self._launch_coro.done()}")
        if self._job_status == SkyFutureStatus.INITIALIZING and self._launch_coro.done():
            self._job_id = self._launch_coro.result()
            self._job_status = SkyFutureStatus.RUNNING

        return self._job_id, self._job_status

class SkyTaskTracker(object):
    _JOB_RESIGTRY: Dict[str, SkyTaskFuture] = {}
    
    @staticmethod
    def register_sky_task(task_template: TaskTemplate):
        new_task = SkyTaskFuture(task_template)
        started = new_task.start()
        if not started:
            logger.warning(f"{new_task.task_name} task has already been created.")
        else:
            SkyTaskTracker._JOB_RESIGTRY[new_task.task_name] = new_task
        return new_task


    @staticmethod  # deprecated
    def get(job_id: int) -> Tuple[int, SkyFutureStatus]:
        return SkyTaskTracker._JOB_RESIGTRY[job_id].done()

def query_job_status(resource_meta: SkyPilotMetadata):
    cluster_records = sky.status(cluster_names=[resource_meta.cluster_name])
    if cluster_records == [] or cluster_records[0]['status'] == sky.ClusterStatus.INIT:
        return sky.JobStatus.INIT
    cluster_status = cluster_records[0]['status']
    assert cluster_status == sky.ClusterStatus.UP
    job_list = sky.queue(resource_meta.cluster_name)
    for job in job_list:
        if job["job_name"] == resource_meta.job_name:
            return job["status"]
        
    return sky.JobStatus.FAILED_SETUP



class SkyPilotAgent(AsyncAgentBase):
    _args = None
    _path_mapping = None
    _cloud_checked: bool = False
    def __init__(self):
        super().__init__(task_type_name=TASK_TYPE, metadata_type=SkyPilotMetadata)
        self._path_mapping = {}
    

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
        logger.warning(f"Creating... SkyPilot {task_template.container.args} | {task_template.container.image}")
        import pdb
        pdb.set_trace()
        task = SkyTaskTracker.register_sky_task(task_template=task_template)
        
        logger.warning(f"Created SkyPilot {task.task_name}")
        # await SkyTaskTracker._JOB_RESIGTRY[job_id]._launch_coro
        return SkyPilotMetadata(
            job_name=task.task_name,
            cluster_name=task_template.custom["cluster_name"],
            output_blob_prefix=task.sky_path_setting.args["output_prefix"],
            sky_zip_prefix=task.sky_path_setting.file_access.raw_output_prefix,
        )
        
        
    def remote_setup(self, remote_meta: SkyPilotMetadata, wrapped, **kwargs):
        sky_path_setting = SkyPathSetting({"raw_output_data_prefix": remote_meta.sky_zip_prefix})
        sky_path_setting.download_and_unzip()
        # mock ssh path
        original_ssh_dirs = _ssh_ctx_Var.get()
        private_key_base = os.path.basename(sky.authentication.PRIVATE_SSH_KEY_PATH)
        public_key_base = os.path.basename(sky.authentication.PUBLIC_SSH_KEY_PATH)
        ssh_ctx_token = _ssh_ctx_Var.set([*original_ssh_dirs, sky_path_setting.local_path_setting.home_key_dir])
        sky.authentication.PRIVATE_SSH_KEY_PATH = os.path.join(ssh_ctx_token, private_key_base)
        sky.authentication.PUBLIC_SSH_KEY_PATH = os.path.join(ssh_ctx_token, public_key_base)
        # mock db path
        original_sky_dirs = _db_dir_ctx_Var.get()
        sky_ctx_token = _db_dir_ctx_Var.set([*original_sky_dirs, sky_path_setting.local_path_setting.home_sky_dir])
        sky.global_user_state._DB = sky.utils.SQLiteConn(
            os.path.join(_db_dir_ctx_Var.get()[-1], "state.db"), 
            sky.global_user_state.create_table
        )
        sky.skylet.job_lib._DB = sky.utils.SQLiteConn(
            os.path.join(_db_dir_ctx_Var.get()[-1], "skylet.db"),
            sky.skylet.job_lib.create_table
        )
        sky.skylet.job_lib._CURSOR = sky.skylet.job_lib._DB.cursor
        sky.skylet.job_lib._CONN = sky.skylet.job_lib._DB.conn
        # run the wrapped function
        wrapped_result = wrapped(**kwargs)
        # reset
        _ssh_ctx_Var.reset(ssh_ctx_token)
        _db_dir_ctx_Var.reset(sky_ctx_token)
        return wrapped_result
    

    async def get(self, resource_meta: SkyPilotMetadata, **kwargs) -> Resource:
        import pdb
        pdb.set_trace()
        job_status = None
        outputs = None
        if resource_meta.job_name not in SkyTaskTracker._JOB_RESIGTRY:
            job_status = self.remote_setup(resource_meta, query_job_status, resource_meta=resource_meta)
        else:
            job_status = query_job_status(resource_meta)
            
        if job_status == sky.JobStatus.SUCCEEDED:
            pass
            # download outputs.pb and convert to LiteralMap
            '''
            import tempfile
            file_access = FileAccessProvider(local_sandbox_dir=tempfile.mkdtemp(prefix="flyte"), raw_output_prefix=resource_meta.output_blob_prefix)
            file_access.get_data(
                file_access.join(resource_meta.output_blob_prefix, "outputs.pb"), 
                os.path.join(file_access.local_sandbox_dir, "outputs.pb")
            )
            output_proto = utils.load_proto_from_file(LiteralMap, os.path.join(file_access.local_sandbox_dir, "outputs.pb"))
            outputs = LiteralMap.from_flyte_idl(output_proto)
            '''
            
        phase = skypilot_status_to_flyte_phase(job_status)

        return Resource(phase=phase, outputs=outputs, message=None)

    async def delete(self, resource_meta: SkyPilotMetadata, **kwargs):
        import pdb
        # pdb.set_trace()
        stopped_cluster = None
        if resource_meta.job_name not in SkyTaskTracker._JOB_RESIGTRY:
            stopped_cluster = self.remote_setup(resource_meta, sky.stop, cluster_name=resource_meta.cluster_name)
        else:
            existed_task = SkyTaskTracker._JOB_RESIGTRY[resource_meta.job_name]
            existed_task.cancel()
            stopped_cluster = sky.stop(resource_meta.cluster_name)
        

# To register the skypilot agent
AgentRegistry.register(SkyPilotAgent())
