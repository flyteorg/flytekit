from typing import Optional, List, Dict, Any, Tuple
from dataclasses import dataclass, asdict
import asyncio
import sky
import sky.check
import sky.cli as sky_cli
import sky.clouds.cloud_registry
import sky.core
import sky.exceptions
import sky.resources
import os
from flytekit.models.literals import LiteralMap
from flytekit import logger, FlyteContextManager
from flytekit.models.task import TaskTemplate
import tarfile
import shutil
from flytekit.extend.backend.base_agent import AsyncAgentBase, AgentRegistry, Resource, ResourceMeta
from flytekitplugins.skypilot.utils import skypilot_status_to_flyte_phase, \
    execute_cmd_to_path, RemoteDeletedError, TaskFutureStatus
from flytekitplugins.skypilot.task import ContainerRunType
from flytekit.core.data_persistence import FileAccessProvider
from flytekit.core import utils
from flyteidl.core import literals_pb2 as _literals_pb2
from flytekitplugins.skypilot.cloud_registry import CloudRegistry, CloudCredentialError, CloudNotInstalledError
from google.protobuf.json_format import MessageToDict
from google.protobuf.struct_pb2 import Struct
import textwrap
import multiprocessing
import functools
import tempfile
import shlex

TASK_TYPE = "skypilot"


@dataclass
class SkyPilotMetadata(ResourceMeta):
    """
    This is the metadata for the job.
    """
    job_name: str
    cluster_name: str
    output_blob_prefix: str
    sky_zip_prefix: str


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
        if image is None and task_template.custom["container_run_type"] == ContainerRunType.RUNTIME:
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
    remote_error_log: str = None
    def __post_init__(self):
        self.remote_sky_dir = self.file_access.join(self.file_access.raw_output_prefix, ".skys")
        self.remote_sky_zip = self.file_access.join(self.remote_sky_dir, "home_sky.tar.gz")
        self.remote_key_zip = self.file_access.join(self.remote_sky_dir, "sky_key.tar.gz")
        self.remote_error_log = self.file_access.join(self.remote_sky_dir, "error.log")
        self.remote_delete_proto = self.file_access.join(self.remote_sky_dir, "delete.pb")

    def remote_exists(self):
        return self.file_access.exists(self.remote_sky_zip)\
            or self.file_access.exists(self.remote_key_zip)
        
    def remote_failed(self):
        return self.file_access.exists(self.remote_error_log)
        
    def remote_delete(self, local_deletion_proto: str):
        self.file_access.put_data(local_deletion_proto, self.remote_delete_proto)
    
    def find_delete_proto(self) -> SkyPilotMetadata:
        if not self.file_access.exists(self.remote_delete_proto):
            return None
        temp_proto = self.file_access.get_random_local_path()
        self.file_access.get_data(self.remote_delete_proto, temp_proto)
        with open(temp_proto, 'rb') as f:
            meta_proto = Struct()
            meta_proto.ParseFromString(f.read())
        
        os.remove(temp_proto)
        return SkyPilotMetadata(**MessageToDict(meta_proto))
        

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
        remote_sep = file_provider.get_filesystem().sep
        self.execution_id = self.file_access.raw_output_prefix.strip(remote_sep).split(remote_sep)[-1]
        self.local_path_setting = LocalPathSetting(file_access=file_provider, execution_id=self.execution_id)
        self.remote_path_setting = RemotePathSetting(file_access=file_provider, execution_id=self.execution_id)
        
    async def zip_and_upload(self, zip_every_n_seconds: int = 5):
        # compress ~/.sky to home_sky.tar.gz
        # put ~/.ssh/sky-key* to sky_key.tar.gz
        logger.warning(self.local_path_setting)
        logger.warning(self.remote_path_setting)
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
            # checks if the task has been deleted
            self.check_deletion()
            await asyncio.sleep(zip_every_n_seconds)


    def check_deletion(self):
        resource_meta = self.remote_path_setting.find_delete_proto()
        if resource_meta is not None:
            raise RemoteDeletedError(f"{resource_meta.job_name} on {resource_meta.cluster_name} has been deleted.")

    def download_and_unzip(self):
        file_sys = self.file_access.get_filesystem()
        remote_basename = self.remote_path_setting.remote_sky_zip.split(file_sys.sep)[-1]
        local_sky_zip = os.path.join(
            os.path.dirname(self.local_path_setting.home_sky_zip), 
            remote_basename
        )
        self.file_access.get_data(self.remote_path_setting.remote_sky_zip, local_sky_zip)
        self.file_access.get_data(self.remote_path_setting.remote_key_zip, self.local_path_setting.sky_key_zip)
        shutil.unpack_archive(local_sky_zip, self.local_path_setting.home_sky_dir)
        with tarfile.open(self.local_path_setting.sky_key_zip, 'r:gz') as key_tar:
            key_tar.extractall(self.local_path_setting.home_key_dir)
        return
    
    
    def put_error_log(self, error_log: Exception):
        log_file = os.path.join(self.local_path_setting.local_sky_prefix, "error.log")
        os.makedirs(os.path.dirname(log_file), exist_ok=True)
        with open(log_file, 'w') as f:
            f.write(str(error_log))
        self.file_access.put_data(log_file, self.remote_path_setting.remote_error_log)
        # delete local log file
        os.remove(log_file)

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
            self.flytekit_pip = textwrap.dedent("""\
                python -m pip uninstall flytekit -y
                python -m pip install -e /flytekit
            """)
            
        self.full_setup = "\n".join(filter(None, [task_setup, self.docker_pull, self.flytekit_pip])).strip()
class RunCommand(object):
    full_task_command: str = None
    def __init__(self, task_template: TaskTemplate) -> None:
        raw_task_command = shlex.join(task_template.container.args)
        local_env_prefix = "\n".join([f"export {k}='{v}'" for k, v in task_template.custom["local_config"]["local_envs"].items()])
        if task_template.custom["container_run_type"] == ContainerRunType.RUNTIME:
            python_path_command = "export PYTHONPATH=$PYTHONPATH:$HOME/sky_workdir"
            self.full_task_command = "\n".join(filter(None, [local_env_prefix, python_path_command, raw_task_command])).strip()
        else:
            container_entrypoint, container_args = task_template.container.args[0], task_template.container.args[1:]
            docker_run_prefix = f"docker run --entrypoint {container_entrypoint}"
            volume_setups, cloud_cred_envs = [], []
            for cloud in CloudRegistry.list_clouds():
                volume_map = cloud.get_mount_envs()
                for env_key, path_mapping in volume_map.items():
                    if os.path.exists(os.path.expanduser(path_mapping.vm_path)):
                        volume_setups.append(f"-v {path_mapping.vm_path}:{path_mapping.container_path}")
                    cloud_cred_envs.append(f"-e {env_key}={path_mapping.container_path}")
            volume_command = " ".join(volume_setups)
            cloud_cred_env_command = " ".join(cloud_cred_envs)
            self.full_task_command = " ".join([
                docker_run_prefix,
                volume_command,
                cloud_cred_env_command,
                task_template.container.image,
                *container_args
            ])    

class SkyTaskFuture(object):
    _job_id: int = -1  # not executed yet
    _task_kwargs: TaskTemplate = None
    _launch_coro: asyncio.Task = None
    _zip_coro: asyncio.Task = None
    _sky_path_setting: SkyPathSetting = None
    _task_name: str = None
    _cluster_name: str = None
    _cloud_checked: bool = False
    def __init__(self, task_template: TaskTemplate):
        self._task_kwargs = task_template
        args = execute_cmd_to_path(task_template.container.args)
        self._sky_path_setting = SkyPathSetting(args=args)
        task_suffix = self._sky_path_setting.execution_id
        self._task_name = f'{self.task_template.custom["task_name"]}.{task_suffix}'
        self._cluster_name = self.task_template.custom["cluster_name"]

    def launch(self):
        if not SkyTaskTracker._cloud_checked and not self.cloud_checked:
            setup_cloud_credential()
            self._cloud_checked = True

        cluster_name: str = self.task_template.custom["cluster_name"]
        # sky_resources: List[Dict[str, str]] = task_template.custom["resource_config"]
        sky_resources = parse_sky_resources(self.task_template)
        # build setup commands
        setup_command = SetupCommand(self.task_template)
        # build run commands
        run_command = RunCommand(self.task_template)
        logger.warning(f"Launching task... \nSetup: \n{setup_command.full_setup}\nRun: \n{run_command.full_task_command}")
        task = sky.Task(run=run_command.full_task_command, name=self._task_name, setup=setup_command.full_setup)
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

    @property
    def cluster_name(self) -> str:
        return self._cluster_name

    @property
    def cloud_checked(self) -> bool:
        return self._cloud_checked

    def launch_failed_callback(self, task: asyncio.Task):
        if task.exception() is not None:
            self.sky_path_setting.put_error_log(task.exception())
            logger.error(
                f"Task {self.task_name} failed to launch.\n"
                f"Cause: \n{task.exception()}\n"
                f"error_log is at {self.sky_path_setting.remote_path_setting.remote_error_log}"
            )
            
    def zip_failed_callback(self, task: asyncio.Task):
        if type(task.exception()) == RemoteDeletedError:
            self.sky_path_setting.put_error_log(task.exception())
            logger.error(
                f"Task {self.task_name} failed to zip and upload.\n"
                f"Cause: \n{task.exception()}\n"
                f"error_log is at {self.sky_path_setting.remote_path_setting.remote_error_log}"
            )
            self.cancel()

    def start(self) -> bool:
        '''
        Before starting, check if the remote ~/.sky and ~/.ssh/sky-key* exists.
        This is to avoid repeated task creation, but the check and upload isn't atomic.
        '''
        if not self.sky_path_setting.remote_path_setting.remote_exists():
            self._launch_coro = asyncio.create_task(asyncio.to_thread(self.launch))
            # self._launch_coro.add_done_callback(helper_done_callback)
            self._launch_coro.add_done_callback(self.launch_failed_callback)
            self._zip_coro = asyncio.create_task(self.sky_path_setting.zip_and_upload())
            return True
        
        return False
    
    def cancel(self):
        if self._launch_coro is not None:
            self._launch_coro.cancel()
        if self._zip_coro is not None:
            self._zip_coro.cancel()
        
        running_tasks_on_cluster = filter(
            lambda task: task.task_name != self.task_name and task.cluster_name == self.cluster_name, 
            SkyTaskTracker._JOB_RESIGTRY.values()
        )
        if not running_tasks_on_cluster:
            sky.down(self.cluster_name)
        
        del SkyTaskTracker._JOB_RESIGTRY[self.task_name]

class SkyTaskTracker(object):
    _JOB_RESIGTRY: Dict[str, SkyTaskFuture] = {}
    _cloud_checked: bool = False
    @staticmethod
    def register_sky_task(task_template: TaskTemplate):
        new_task = SkyTaskFuture(task_template)
        started = new_task.start()
        SkyTaskTracker._cloud_checked = SkyTaskTracker._cloud_checked or \
            any([task.cloud_checked for task in SkyTaskTracker._JOB_RESIGTRY.values()])
        if not started:
            logger.warning(f"{new_task.task_name} task has already been created.")
        else:
            SkyTaskTracker._JOB_RESIGTRY[new_task.task_name] = new_task
        return new_task

def query_job_status(resource_meta: SkyPilotMetadata):
    # task on another agent pod may fail to launch, check for launch error log
    sky_path_setting = SkyPathSetting({"raw_output_data_prefix": resource_meta.sky_zip_prefix})
    if sky_path_setting.remote_path_setting.remote_failed():
        local_log_file = sky_path_setting.file_access.get_random_local_path()
        sky_path_setting.file_access.get_data(sky_path_setting.remote_path_setting.remote_error_log, local_log_file)
        with open(local_log_file, 'r') as f:
            logger.error(f.read())
        os.remove(local_log_file)
        return sky.JobStatus.FAILED_SETUP
    
    # check job status
    cluster_records = sky.status(cluster_names=[resource_meta.cluster_name])
    if cluster_records == [] or cluster_records[0]['status'] == sky.ClusterStatus.INIT:
        return sky.JobStatus.INIT
    cluster_status = cluster_records[0]['status']
    # TODO: fix coro error parsing
    assert cluster_status == sky.ClusterStatus.UP
    job_list = sky.queue(resource_meta.cluster_name)
    for job in job_list:
        if job["job_name"] == resource_meta.job_name:
            return job["status"]
        
    return sky.JobStatus.FAILED_SETUP

def remote_deletion(resource_meta: SkyPilotMetadata):
    sky_settings = SkyPathSetting({"raw_output_data_prefix": resource_meta.sky_zip_prefix})
    meta_proto = Struct()
    meta_proto.update(resource_meta.__dict__)
    # save to meta_proto to proto file
    with tempfile.NamedTemporaryFile(mode="wb") as f:
        f.write(meta_proto.SerializeToString())
        f.flush()
        sky_settings.remote_path_setting.remote_delete(f.name)

    

class SkyPilotAgent(AsyncAgentBase):
    _args = None
    _path_mapping = None
    def __init__(self):
        super().__init__(task_type_name=TASK_TYPE, metadata_type=SkyPilotMetadata)
        self._path_mapping = {}
    

    async def create(
        self,
        task_template: TaskTemplate,
        inputs: Optional[LiteralMap] = None,
        **kwargs,
    ) -> SkyPilotMetadata:

        # ctx = FlyteContextManager.current_context()
        logger.warning(f"Creating... SkyPilot {task_template.container.args} | {task_template.container.image}")
        import pdb
        # pdb.set_trace()
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
        home_sky_dir = sky_path_setting.local_path_setting.home_sky_dir
        home_key_dir = sky_path_setting.local_path_setting.home_key_dir
        # mock ssh path
        private_key_base = os.path.basename(sky.authentication.PRIVATE_SSH_KEY_PATH)
        public_key_base = os.path.basename(sky.authentication.PUBLIC_SSH_KEY_PATH)
        sky.authentication.PRIVATE_SSH_KEY_PATH = os.path.join(home_key_dir, private_key_base)
        sky.authentication.PUBLIC_SSH_KEY_PATH = os.path.join(home_key_dir, public_key_base)
        # mock db path
        sky.global_user_state._DB = sky.utils.db_utils.SQLiteConn(
            os.path.join(home_sky_dir, "state.db"), 
            sky.global_user_state.create_table
        )
        sky.skylet.job_lib._DB = sky.utils.db_utils.SQLiteConn(
            os.path.join(home_sky_dir, "skylet.db"),
            sky.skylet.job_lib.create_table
        )
        sky.skylet.job_lib._CURSOR = sky.skylet.job_lib._DB.cursor
        sky.skylet.job_lib._CONN = sky.skylet.job_lib._DB.conn
        # mock sky.backends.backend_utils
        sky.backends.backend_utils.SKY_USER_FILE_PATH = os.path.join(home_sky_dir, "generated")
        sky.backends.backend_utils.SKY_REMOTE_PATH = os.path.join(home_sky_dir, "wheels")
        sky.backends.backend_utils.SKY_REMOTE_APP_DIR = os.path.join(home_sky_dir, "sky_app")
        # run the wrapped function
        wrapped_result = wrapped(**kwargs)
        return wrapped_result
    

    async def get(self, resource_meta: SkyPilotMetadata, **kwargs) -> Resource:
        import pdb
        # pdb.set_trace()
        job_status = None
        outputs = None
        if resource_meta.job_name not in SkyTaskTracker._JOB_RESIGTRY:
            # use pool to separate the sky directory
            with multiprocessing.Pool(1) as p:
                starmap_results = p.starmap(
                    functools.partial(self.remote_setup, resource_meta=resource_meta), 
                    [(resource_meta, query_job_status)]
                )
                job_status = starmap_results[0]
            # logger.warning(f"after setup:{sky.authentication.PRIVATE_SSH_KEY_PATH}")
        else:
            job_status = query_job_status(resource_meta)
            
        phase = skypilot_status_to_flyte_phase(job_status)
        return Resource(phase=phase, outputs=outputs, message=None)

    async def delete(self, resource_meta: SkyPilotMetadata, **kwargs):
        # FIXME: the deleting is called multiple times even if I finished deletion
        import pdb
        # pdb.set_trace()
        stopped_cluster = None
        if resource_meta.job_name not in SkyTaskTracker._JOB_RESIGTRY:
            remote_deletion(resource_meta)
            
        else:
            existed_task = SkyTaskTracker._JOB_RESIGTRY[resource_meta.job_name]
            existed_task.cancel()
            # stopped_cluster = sky.down(resource_meta.cluster_name)
            
        

# To register the skypilot agent
AgentRegistry.register(SkyPilotAgent())
