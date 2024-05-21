from asyncio.subprocess import PIPE
from decimal import ROUND_CEILING, Decimal
from typing import Optional, Tuple, Any, Dict, Union
import asyncio
from flyteidl.core.execution_pb2 import TaskExecution
from typing import List
from flytekit import logger
import flytekit
import enum
from flytekit.core.resources import Resources
from flytekit.tools.fast_registration import download_distribution as _download_distribution
from flytekitplugins.skypilot.metadata import SkyPilotMetadata, JobLaunchType, ContainerRunType
from flytekitplugins.skypilot.cloud_registry import CloudRegistry, CloudCredentialError, CloudNotInstalledError
from flytekit.models.task import TaskTemplate
import sky
import pathlib
from datetime import datetime
import os
import rich_click as _click
from  dataclasses import dataclass, field, asdict
from google.protobuf.struct_pb2 import Struct
from google.protobuf.json_format import MessageToDict
import tarfile
import tempfile
import shutil
from flytekit.core.data_persistence import FileAccessProvider

SKYPILOT_STATUS_TO_FLYTE_PHASE = {
    "INIT": TaskExecution.INITIALIZING,
    "PENDING": TaskExecution.INITIALIZING,
    "SETTING_UP": TaskExecution.INITIALIZING,
    "RUNNING": TaskExecution.RUNNING,
    "SUCCEEDED": TaskExecution.SUCCEEDED,
    "FAILED": TaskExecution.FAILED,
    "FAILED_SETUP": TaskExecution.FAILED,
    "CANCELLED": TaskExecution.FAILED,
    "STARTING": TaskExecution.INITIALIZING,
    "RECOVERING": TaskExecution.WAITING_FOR_RESOURCES,
    "CANCELLING": TaskExecution.WAITING_FOR_RESOURCES,
    "CANCELLED": TaskExecution.ABORTED,
    "FAILED_PRECHECKS": TaskExecution.FAILED,
    "FAILED_NO_RESOURCE": TaskExecution.FAILED,
    "FAILED_CONTROLLER": TaskExecution.FAILED,
    "SUBMITTED": TaskExecution.QUEUED,
}


def skypilot_status_to_flyte_phase(status: sky.JobStatus) -> TaskExecution.Phase:
    """
    Map Skypilot status to Flyte phase.
    """
    return SKYPILOT_STATUS_TO_FLYTE_PHASE[status.value]


LAUNCH_TYPE_TO_SKY_STATUS: dict[int, Union[sky.JobStatus, sky.jobs.ManagedJobStatus]] = {
    JobLaunchType.NORMAL: sky.JobStatus,
    JobLaunchType.MANAGED: sky.jobs.ManagedJobStatus,
}

# use these commands from entrypoint to help resolve the task_template.container.args
@_click.group()
def _pass_through():
    pass


@_pass_through.command("pyflyte-execute")
@_click.option("--inputs", required=True)
@_click.option("--output-prefix", required=True)
@_click.option("--raw-output-data-prefix", required=False)
@_click.option("--checkpoint-path", required=False)
@_click.option("--prev-checkpoint", required=False)
@_click.option("--test", is_flag=True)
@_click.option("--dynamic-addl-distro", required=False)
@_click.option("--dynamic-dest-dir", required=False)
@_click.option("--resolver", required=False)
@_click.argument(
    "resolver-args",
    type=_click.UNPROCESSED,
    nargs=-1,
)
def execute_task_cmd(
    inputs,
    output_prefix,
    raw_output_data_prefix,
    test,
    prev_checkpoint,
    checkpoint_path,
    dynamic_addl_distro,
    dynamic_dest_dir,
    resolver,
    resolver_args,
):
    pass


@_pass_through.command("pyflyte-fast-execute")
@_click.option("--additional-distribution", required=False)
@_click.option("--dest-dir", required=False)
@_click.argument("task-execute-cmd", nargs=-1, type=_click.UNPROCESSED)
def fast_execute_task_cmd(additional_distribution: str, dest_dir: str, task_execute_cmd: List[str]):
    # Insert the call to fast before the unbounded resolver args
    if additional_distribution is not None:
        if not dest_dir:
            dest_dir = os.getcwd()
        # _download_distribution(additional_distribution, dest_dir)

    # Insert the call to fast before the unbounded resolver args
    cmd = []
    for arg in task_execute_cmd:
        if arg == "--resolver":
            cmd.extend(["--dynamic-addl-distro", additional_distribution, "--dynamic-dest-dir", dest_dir])
        cmd.append(arg)
        
    return cmd
     


@_pass_through.command("pyflyte-map-execute")
@_click.option("--inputs", required=True)
@_click.option("--output-prefix", required=True)
@_click.option("--raw-output-data-prefix", required=False)
@_click.option("--max-concurrency", type=int, required=False)
@_click.option("--test", is_flag=True)
@_click.option("--dynamic-addl-distro", required=False)
@_click.option("--dynamic-dest-dir", required=False)
@_click.option("--resolver", required=True)
@_click.option("--checkpoint-path", required=False)
@_click.option("--prev-checkpoint", required=False)
@_click.argument(
    "resolver-args",
    type=_click.UNPROCESSED,
    nargs=-1,
)
def map_execute_task_cmd(
    inputs,
    output_prefix,
    raw_output_data_prefix,
    max_concurrency,
    test,
    dynamic_addl_distro,
    dynamic_dest_dir,
    resolver,
    resolver_args,
    prev_checkpoint,
    checkpoint_path,
):
    pass


ENTRYPOINT_MAP = {
    execute_task_cmd.name: execute_task_cmd,
    fast_execute_task_cmd.name: fast_execute_task_cmd,
    map_execute_task_cmd.name: map_execute_task_cmd,
}

def execute_cmd_to_path(cmd: List[str]) -> Dict[str, Any]:
    assert len(cmd) > 0
    args = {}
    for entrypoint_name, cmd_entrypoint in ENTRYPOINT_MAP.items():
        if entrypoint_name == cmd[0]:
            ctx = cmd_entrypoint.make_context(info_name="", args=cmd[1:])
            args.update(ctx.params)
            if cmd_entrypoint.name == fast_execute_task_cmd.name:
                args = {}
                pyflyte_args = fast_execute_task_cmd.invoke(ctx)
                pyflyte_ctx = ENTRYPOINT_MAP[pyflyte_args[0]].make_context(
                    info_name="", 
                    args=list(pyflyte_args)[1:]
                )
                args.update(pyflyte_ctx.params)
                # args["full-command"] = pyflyte_args
            break
    
    # raise error if args is empty or cannot find raw_output_data_prefix
    if not args or args.get("raw_output_data_prefix", None) is None:
        raise ValueError(f"Bad command for {cmd}")
    return args
        
        
class RemoteDeletedError(ValueError):
    """
    This is the base error for cloud credential errors.
    """
    pass

@dataclass
class TaskFutureStatus(object):
    """
    This is the status for the task future.
    """
    job_type: int
    cluster_status: str = sky.ClusterStatus.INIT.value
    task_status: str = sky.JobStatus.PENDING.value
    


SKY_DIRNAME = "skypilot_agent"
@dataclass
class BaseRemotePathSetting:
    file_access: FileAccessProvider
    remote_sky_dir: str = field(init=False)
    def __post_init__(self):
        self.remote_sky_dir = self.file_access.join(self.file_access.raw_output_prefix, ".skys")

    def remote_exists(self):
        raise NotImplementedError

    def remote_failed(self):
        raise NotImplementedError
        
    def find_delete_proto(self) -> Dict[str, Any]:
        raise NotImplementedError
    
    def touch_task(self, task_id: str):
        raise NotImplementedError
        
@dataclass
class TrackerRemotePathSetting(BaseRemotePathSetting):
    # raw_output_prefix: s3://{bucket}/{SKY_DIRNAME}/{hostname}
    unique_id: str
    _HOME_SKY = "home_sky.tar.gz"
    _KEY_SKY = "sky_key.tar.gz"
    def __post_init__(self):
        super().__post_init__()
        self.remote_sky_zip = self.file_access.join(self.remote_sky_dir, self._HOME_SKY)
        self.remote_key_zip = self.file_access.join(self.remote_sky_dir, self._KEY_SKY)

    def remote_exists(self):
        return self.file_access.exists(self.remote_sky_zip)\
            or self.file_access.exists(self.remote_key_zip)
        
    def delete_task(self, task_id: str):
        self.file_access.raw_output_fs.rm_file(self.file_access.join(self.remote_sky_dir, task_id))
        
    def touch_task(self, task_id: str):
        self.file_access.raw_output_fs.touch(self.file_access.join(self.remote_sky_dir, task_id))
        
    def task_exists(self, task_id: str):
        file_system = self.file_access.raw_output_fs
        remote_sky_parts = self.remote_sky_dir.rstrip(file_system.sep).split(file_system.sep)
        base_host_dir = file_system.sep.join(remote_sky_parts[:-1])
        hosts = file_system.ls(base_host_dir)
        # find task_id in the host directory
        for host in hosts:
            registered_tasks = file_system.ls(host)
            for task in registered_tasks:
                if task_id in task:
                    return True

        return False
@dataclass
class TaskRemotePathSetting(BaseRemotePathSetting):
    # raw_output_prefix: s3://{bucket}/data/.../{id}
    job_type: JobLaunchType
    cluster_name: str
    unique_id: str = None
    task_id: int = None
    task_name: str = None
    
    
    def from_task_prefix(self, task_prefix: str):
        task_name = f"{task_prefix}.{self.unique_id}"
        return TaskRemotePathSetting(
            file_access=self.file_access,
            job_type=self.job_type,
            cluster_name=self.cluster_name,
            task_name=task_name
        )
    

    def __post_init__(self):
        super().__post_init__()
        self.remote_error_log = self.file_access.join(self.remote_sky_dir, "error.log")
        self.remote_delete_proto = self.file_access.join(self.remote_sky_dir, "delete.pb")
        self.remote_status_proto = self.file_access.join(self.remote_sky_dir, "status.pb")
        file_system = self.file_access.get_filesystem()
        sep = file_system.sep
        # get last part of the path as unique_id
        self.unique_id = self.file_access.raw_output_prefix.rstrip(sep).split(sep)[-1]
        logger.warning(self)
        

    def remote_failed(self):
        return self.file_access.exists(self.remote_error_log)

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
    
    def put_error_log(self, error_log: Exception):
        # open file for write and read
        with tempfile.NamedTemporaryFile(mode='w+', delete=False) as log_file:
            log_file.write(str(error_log))
            log_file.flush()
            log_file.seek(0)
            self.file_access.put_data(log_file.name, self.remote_error_log)

    def delete_job(self):
        status = self.get_task_status()
        job_status = LAUNCH_TYPE_TO_SKY_STATUS[status.job_type](status.task_status)
        if status.cluster_status == sky.ClusterStatus.INIT.value:
            # can't delete job if cluster is not up
            return
        
        if not self.task_id:
            # task hasn't been submitted, try query again
            try:
                loop = asyncio.get_event_loop()
                job_list = asyncio.run_coroutine_threadsafe(self.get_job_list(), loop).result()
            finally:
                pass
            job_status, self.task_id = self.get_status_from_list(job_list)
        if job_status is None or self.task_id is None:
            # task hasn't been submitted
            return
        if not job_status.is_terminal():
            if self.job_type == JobLaunchType.NORMAL:
                sky.cancel(cluster_name=self.cluster_name, job_ids=[self.task_id], _try_cancel_if_cluster_is_init=True)
            else:
                sky.jobs.cancel(name=self.task_name, job_ids=[self.task_id])

    def to_proto_and_upload(self, target, remote_path):
        proto_struct = Struct()
        proto_struct.update(asdict(target))
        with tempfile.NamedTemporaryFile(mode='wb') as f:
            f.write(proto_struct.SerializeToString())
            f.flush()
            f.seek(0)
            self.file_access.put_data(f.name, remote_path)

    def get_task_status(self):
        temp_proto = self.file_access.get_random_local_path()
        self.file_access.get_data(self.remote_status_proto, temp_proto)
        with open(temp_proto, 'rb') as f:
            status_proto = Struct()
            status_proto.ParseFromString(f.read())
        os.remove(temp_proto)
        status = TaskFutureStatus(**MessageToDict(status_proto))
        if self.remote_failed():
            local_log_file = self.file_access.get_random_local_path()
            self.file_access.get_data(self.remote_error_log, local_log_file)
            with open(local_log_file, 'r') as f:
                logger.error(f.read())
            os.remove(local_log_file)
            return TaskFutureStatus(status.job_type, status.cluster_status, sky.JobStatus.FAILED.value)
        return status

    async def get_job_list(self) -> List[Dict[str, Any]]:
        if self.job_type == JobLaunchType.NORMAL:
            return await asyncio.to_thread(sky.queue, self.cluster_name)
        return await asyncio.to_thread(sky.jobs.queue, refresh=True)

    def get_status_from_list(self, job_list) -> Tuple[sky.JobStatus, Optional[int]]:
        for job in job_list:
            if job["job_name"] == self.task_name:
                return job["status"], job["job_id"]
        return None, None

    async def put_task_status(self, put_every_n_seconds: int = 5):
        init_status = TaskFutureStatus(job_type=self.job_type)
        self.to_proto_and_upload(init_status, self.remote_status_proto)
        # FIXME too long
        while True:
            task_id = None
            # pdb.set_trace()
            prev_status = self.get_task_status()
            status = prev_status
            if LAUNCH_TYPE_TO_SKY_STATUS[self.job_type](prev_status.task_status).is_terminal():
                return
            if prev_status.cluster_status == sky.ClusterStatus.UP.value:
                job_list = await self.get_job_list()
                current_status, task_id = self.get_status_from_list(job_list)
                if current_status:
                    current_status = current_status.value
                status = TaskFutureStatus(
                    job_type=self.job_type,
                    cluster_status=prev_status.cluster_status,
                    task_status=current_status or prev_status.task_status
                )
                logger.warning(status)
                        
            else:
                cluster_status = sky.status(self.cluster_name)
                if not cluster_status:
                    # starts putting but cluster haven't launched
                    cluster_status = [{"status": sky.ClusterStatus(init_status.cluster_status)}]
                status = TaskFutureStatus(
                    job_type=self.job_type,
                    cluster_status=cluster_status[0]['status'].value,
                    task_status=prev_status.task_status
                )
            
            self.task_id = task_id or self.task_id
            self.to_proto_and_upload(status, self.remote_status_proto)
            await asyncio.sleep(put_every_n_seconds)
        
    async def deletion_status(self, check_every_n_seconds: int = 5):
        # checks if the task has been deleted from another pod
        while True:
            self.check_deletion()
            await asyncio.sleep(check_every_n_seconds)
    
    def check_deletion(self):
        resource_meta = self.find_delete_proto()
        if resource_meta is not None:
            raise RemoteDeletedError(f"{resource_meta.job_name} on {resource_meta.cluster_name} has been deleted.")

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


    def zip_sky_info(self):
        # compress ~/.sky to home_sky.tar.gz
        sky_zip = shutil.make_archive(self.home_sky_zip, 'gztar', os.path.expanduser("~/.sky"))
        # tar ~/.ssh/sky-key* to sky_key.tar.gz
        local_key_dir = os.path.expanduser("~/.ssh")
        archived_keys = [file for file in os.listdir(local_key_dir) if file.startswith("sky-key")]
        with tarfile.open(self.sky_key_zip, 'w:gz') as key_tar:
            for key_file in archived_keys:
                key_tar.add(os.path.join(local_key_dir, key_file), arcname=os.path.basename(key_file))
        return sky_zip

@dataclass
class SkyPathSetting:
    task_level_prefix: str  # for the filesystem parsing
    unique_id: str
    working_dir: str = None
    local_path_setting: LocalPathSetting = None
    remote_path_setting: TrackerRemotePathSetting = None
    file_access: FileAccessProvider = None
    def __post_init__(self):
        file_provider = FileAccessProvider(local_sandbox_dir="/tmp", raw_output_prefix=self.task_level_prefix)
        bucket_name = file_provider.raw_output_fs._strip_protocol(file_provider.raw_output_prefix)\
                        .split(file_provider.raw_output_fs.sep)[0]
        working_dir = file_provider.join(
            file_provider.raw_output_fs.unstrip_protocol(bucket_name), 
            SKY_DIRNAME, 
        )
        
        self.working_dir = file_provider.join(working_dir, self.unique_id)
        self.file_access = FileAccessProvider(local_sandbox_dir="/tmp", raw_output_prefix=self.working_dir)
        self.file_access.raw_output_fs.mkdir(self.working_dir)
        self.local_path_setting = LocalPathSetting(file_access=self.file_access, execution_id=self.unique_id)
        self.remote_path_setting = TrackerRemotePathSetting(file_access=self.file_access, unique_id=self.unique_id)

    async def zip_and_upload(self, zip_every_n_seconds: int = 5):
        # put ~/.ssh/sky-key* to sky_key.tar.gz
        logger.warning(self.local_path_setting)
        logger.warning(self.remote_path_setting)
        while True:
            sky_zip = self.local_path_setting.zip_sky_info()
            self.file_access.put_data(sky_zip, self.remote_path_setting.remote_sky_zip)
            self.file_access.put_data(self.local_path_setting.sky_key_zip, self.remote_path_setting.remote_key_zip)
            await asyncio.sleep(zip_every_n_seconds)


    def download_and_unzip(self):
        local_sky_zip = os.path.join(
            os.path.dirname(self.local_path_setting.home_sky_zip), 
            self.remote_path_setting._HOME_SKY
        )
        self.file_access.get_data(self.remote_path_setting.remote_sky_zip, local_sky_zip)
        self.file_access.get_data(self.remote_path_setting.remote_key_zip, self.local_path_setting.sky_key_zip)
        shutil.unpack_archive(local_sky_zip, self.local_path_setting.home_sky_dir)
        with tarfile.open(self.local_path_setting.sky_key_zip, 'r:gz') as key_tar:
            key_tar.extractall(self.local_path_setting.home_key_dir)
        return
    
    
    def last_upload_time(self) -> Optional[datetime]:
        if self.file_access.exists(self.remote_path_setting.remote_sky_zip):
            self.file_access.raw_output_fs.ls(self.remote_path_setting.remote_sky_zip, refresh=True)
            return self.file_access.raw_output_fs.info(self.remote_path_setting.remote_sky_zip)['LastModified'] 
    

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


def parse_sky_resources(task_template: TaskTemplate) -> Dict[str, Any]:
    sky_task_config = {}
    resources: Dict[str, Any] = task_template.custom["resource_config"]
    container_image: str = task_template.container.image
    if resources.get('image_id', None) is None and task_template.custom["container_run_type"] == ContainerRunType.RUNTIME:
        resources["image_id"] = f"docker:{container_image}"
        
    sky_task_config.update({
        "resources": resources,
        "file_mounts": task_template.custom["file_mounts"],
    })
    return sky_task_config
    
