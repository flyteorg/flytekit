import asyncio
import enum
import multiprocessing.pool
import os
import shutil
import tarfile
import tempfile
from collections import OrderedDict
import concurrent.futures
from dataclasses import asdict, dataclass, field
from datetime import datetime
import fsspec
import functools
import multiprocessing
from typing import Any, Dict, List, Optional, Tuple, Union, Callable
import traceback
import rich_click as _click
import sky
import sqlite3
from flytekit.models.task import TaskExecutionMetadata
from flyteidl.core.execution_pb2 import TaskExecution
from flytekitplugins.skypilot.cloud_registry import CloudCredentialError, CloudNotInstalledError, CloudRegistry
from flytekitplugins.skypilot.metadata import JobLaunchType, SkyPilotMetadata
from flytekitplugins.skypilot.process_utils import EventHandler, BlockingProcessHandler, ClusterEventHandler
from flytekitplugins.skypilot.constants import SKY_DIRNAME, COROUTINE_INTERVAL, QUEUE_TIMEOUT
from google.protobuf.json_format import MessageToDict
from google.protobuf.struct_pb2 import Struct

from flytekit import logger
from flytekit.core.data_persistence import FileAccessProvider

SKYPILOT_STATUS_TO_FLYTE_PHASE = {
    "INIT": TaskExecution.INITIALIZING,
    "PENDING": TaskExecution.INITIALIZING,
    "SETTING_UP": TaskExecution.INITIALIZING,
    "RUNNING": TaskExecution.RUNNING,
    "SUCCEEDED": TaskExecution.SUCCEEDED,
    "FAILED": TaskExecution.FAILED,
    "FAILED_SETUP": TaskExecution.FAILED,
    "STARTING": TaskExecution.INITIALIZING,
    "RECOVERING": TaskExecution.WAITING_FOR_RESOURCES,
    "CANCELLING": TaskExecution.WAITING_FOR_RESOURCES,
    "CANCELLED": TaskExecution.ABORTED,
    "FAILED_PRECHECKS": TaskExecution.FAILED,
    "FAILED_NO_RESOURCE": TaskExecution.FAILED,
    "FAILED_CONTROLLER": TaskExecution.FAILED,
    "SUBMITTED": TaskExecution.QUEUED,
}


LAUNCH_TYPE_TO_SKY_STATUS: dict[int, Union[sky.JobStatus, sky.jobs.ManagedJobStatus]] = {
    JobLaunchType.NORMAL: sky.JobStatus,
    JobLaunchType.MANAGED: sky.jobs.ManagedJobStatus,
}


def skypilot_status_to_flyte_phase(status: sky.JobStatus) -> TaskExecution.Phase:
    """
    Map Skypilot status to Flyte phase.
    """
    return SKYPILOT_STATUS_TO_FLYTE_PHASE[status.value]

class RemoteDeletedError(ValueError):
    """
    This is the base error for cloud credential errors.
    """

    pass
class QueryState(enum.Enum):
    QUERYING = enum.auto()
    FINISHED = enum.auto()
    
class QueryCache:
    def __init__(self, state=QueryState.FINISHED, last_result=None):
        self.state = state
        self.last_modified = datetime.now()
        self.last_result = last_result
        
    def __repr__(self):
        return f"QueryCache(state={self.state}, last_modified={self.last_modified}, last_result={self.last_result})"
    
    def need_renew(self) -> bool:
        query_result = self.last_result
        if query_result is None:
            return True
        if self.state == QueryState.QUERYING:
            return False
        return (datetime.now() - self.last_modified).seconds > COROUTINE_INTERVAL
    
    def update(self, state: QueryState, last_result: Any):
        self.state = state
        self.last_result = last_result
        self.last_modified = datetime.now()
    
class LRUCache:
    def __init__(self, capacity: int) -> None:
        self.capacity = capacity
        self.cache = OrderedDict()

    def get(self, key: str) -> Optional[QueryCache]:
        if key in self.cache:
            self.cache.move_to_end(key)
            return self.cache[key]
        return None

    def put(self, key: str, value: QueryCache) -> None:
        if key in self.cache:
            self.cache.move_to_end(key)
        elif len(self.cache) >= self.capacity:
            self.cache.popitem(last=False)
        self.cache[key] = value

    def __repr__(self) -> str:
        return str(self.cache)

    def need_renew(self, key: str) -> bool:
        query_result = self.get(key)
        if query_result is None:
            return True
        if query_result.state == QueryState.QUERYING:
            return False
        if query_result.last_result is None:
            return True
        return (datetime.now() - query_result.last_modified).seconds > COROUTINE_INTERVAL

QUEUECACHE = LRUCache(1000)
STATUSCACHE = LRUCache(1000)
@dataclass
class TaskFutureStatus(object):
    """
    This is the status for the task future.
    """

    job_type: int
    cluster_status: str = sky.ClusterStatus.INIT.value
    task_status: str = sky.JobStatus.PENDING.value


class TaskStatus(int, enum.Enum):
    INIT = 0
    CLUSTER_UP = 1
    TASK_SUBMITTED = 2
    DONE = 3


@dataclass
class BaseRemotePathSetting:
    file_access: FileAccessProvider
    remote_sky_dir: str = field(init=False)

    def __post_init__(self):
        self.remote_sky_dir = self.file_access.join(self.file_access.raw_output_prefix, ".skys")
        self.file_access.raw_output_fs.makedirs(self.remote_sky_dir, exist_ok=True)

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
        return self.file_access.exists(self.remote_sky_zip) or self.file_access.exists(self.remote_key_zip)

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
    sky_task_id: int = None
    task_name: str = None

    def from_task_prefix(self, task_prefix: str):
        task_name = f"{task_prefix}.{self.unique_id}"
        return TaskRemotePathSetting(
            file_access=self.file_access, 
            job_type=self.job_type, 
            cluster_name=self.cluster_name, 
            task_name=task_name,
            unique_id=self.unique_id
        )

    def __post_init__(self):
        super().__post_init__()
        self.remote_error_log = self.file_access.join(self.remote_sky_dir, "error.log")
        self.remote_delete_proto = self.file_access.join(self.remote_sky_dir, "delete.pb")
        self.remote_status_proto = self.file_access.join(self.remote_sky_dir, "status.pb")
        logger.warning(self)

    def remote_failed(self):
        return self.file_access.exists(self.remote_error_log)

    def find_delete_proto(self) -> SkyPilotMetadata:
        if not self.file_access.exists(self.remote_delete_proto):
            return None
        temp_proto = self.file_access.get_random_local_path()
        self.file_access.get_data(self.remote_delete_proto, temp_proto)
        with open(temp_proto, "rb") as f:
            meta_proto = Struct()
            meta_proto.ParseFromString(f.read())

        os.remove(temp_proto)
        return SkyPilotMetadata(**MessageToDict(meta_proto))

    def put_error_log(self, error_log: Exception):
        # open file for write and read
        with tempfile.NamedTemporaryFile(mode="w+", delete=False) as log_file:
            log_file.write(str(error_log))
            log_file.flush()
            log_file.seek(0)
            self.file_access.put_data(log_file.name, self.remote_error_log)

    def delete_job(self):
        status = self.get_task_status()
        job_status = LAUNCH_TYPE_TO_SKY_STATUS[status.job_type](status.task_status)
        if not self.sky_task_id:
            # task hasn't been submitted, try query again (this rarely happens)
            try:
                loop = asyncio.get_event_loop()
                job_list = asyncio.run_coroutine_threadsafe(self.get_job_list(), loop).result()
            finally:
                pass
            job_status, self.sky_task_id = self.get_status_from_list(job_list)
        if job_status is None or self.sky_task_id is None:
            # task hasn't been submitted
            return
        if not job_status.is_terminal():
            try:
                if self.job_type == JobLaunchType.NORMAL:
                    sky.cancel(
                        cluster_name=self.cluster_name, job_ids=[self.sky_task_id], _try_cancel_if_cluster_is_init=True
                    )
                else:
                    sky.jobs.cancel(name=self.task_name)
            except sky.exceptions.ClusterNotUpError:
                # no resource leaked
                pass

    def to_proto_and_upload(self, target, remote_path):
        proto_struct = Struct()
        proto_struct.update(asdict(target))
        with tempfile.NamedTemporaryFile(mode="wb") as f:
            f.write(proto_struct.SerializeToString())
            f.flush()
            f.seek(0)
            self.file_access.put_data(f.name, remote_path)

    def get_task_status(self):
        temp_proto = self.file_access.get_random_local_path()
        self.file_access.get_data(self.remote_status_proto, temp_proto)
        with open(temp_proto, "rb") as f:
            status_proto = Struct()
            status_proto.ParseFromString(f.read())
        os.remove(temp_proto)
        status = TaskFutureStatus(**MessageToDict(status_proto))
        if self.remote_failed():
            local_log_file = self.file_access.get_random_local_path()
            self.file_access.get_data(self.remote_error_log, local_log_file)
            with open(local_log_file, "r") as f:
                logger.error(f.read())
            os.remove(local_log_file)
            return TaskFutureStatus(status.job_type, status.cluster_status, sky.JobStatus.FAILED.value)
        return status

    async def get_job_list(self, query_cache: QueryCache) -> List[Dict[str, Any]]:
        queue_key = self.cluster_name if self.job_type == JobLaunchType.NORMAL else "$job$"
        need_renew = QUEUECACHE.need_renew(queue_key)
        prev_queue = QUEUECACHE.get(queue_key)
        logger.warning(f"cache {'miss' if need_renew else 'hit'} {queue_key} | {prev_queue}")
        if not need_renew:
            return prev_queue.last_result
        QUEUECACHE.put(queue_key, QueryCache(state=QueryState.QUERYING, last_result=getattr(prev_queue, "last_result", None)))
        queue_result = None
        if self.job_type == JobLaunchType.NORMAL:
            queue_result = await asyncio.to_thread(
                retry_handler(
                    sky.queue, 
                    [SkyErrorHandler(sqlite3.OperationalError, sqliteErrorHandler), SkyErrorHandler(sky.exceptions.ClusterNotUpError, lambda :None, True)]
                ), 
                self.cluster_name
            )
            
        else:
            queue_result = await asyncio.to_thread(
                retry_handler(
                    sky.jobs.queue, 
                    [SkyErrorHandler(sqlite3.OperationalError, sqliteErrorHandler), SkyErrorHandler(sky.exceptions.ClusterNotUpError, lambda :None, True)]
                ), 
                refresh=True
            )
        QUEUECACHE.put(queue_key, QueryCache(state=QueryState.FINISHED, last_result=queue_result))
        return queue_result or []

    def get_status_from_list(self, job_list) -> Tuple[sky.JobStatus, Optional[int]]:
        for job in job_list:
            if job["job_name"] == self.task_name:
                return job["status"], job["job_id"]
        return None, None


    async def put_task_status(self, event_handler: EventHandler, job_list: List[Dict[str, Any]]) -> bool:
        if event_handler.is_terminal():
            self.handle_return(event_handler)
            return True
        task_id = None 
        prev_status = self.get_task_status()
        status = prev_status
        logger.warning(event_handler)
        # task finished
        if LAUNCH_TYPE_TO_SKY_STATUS[self.job_type](prev_status.task_status).is_terminal():
            event_handler.finished_event.set()
            return True
        # task submitted, can get task_id now
        if event_handler.launch_done_event.is_set():
            # since this doesn't guarantee the cluster is up, we need to check the cluster status
            current_status, task_id = self.get_status_from_list(job_list)
            if current_status:
                current_status = current_status.value
            status = TaskFutureStatus(
                job_type=self.job_type,
                cluster_status=prev_status.cluster_status,
                task_status=current_status or prev_status.task_status,
            )
            logger.warning(status)
            self.sky_task_id = task_id or self.sky_task_id
        self.to_proto_and_upload(status, self.remote_status_proto)
        return False

    async def _put_task_status(self, event_handler: EventHandler) -> bool:
        init_status = TaskFutureStatus(job_type=self.job_type)
        if event_handler.is_terminal():
            self.handle_return(event_handler)
            return True
        task_id = None 
        prev_status = self.get_task_status()
        status = prev_status
        logger.warning(event_handler)
        # task finished
        if LAUNCH_TYPE_TO_SKY_STATUS[self.job_type](prev_status.task_status).is_terminal():
            event_handler.finished_event.set()
            return True
        # task submitted, can get task_id now
        if event_handler.launch_done_event.is_set():
            # since this doesn't guarantee the cluster is up, we need to check the cluster status
            job_list = await self.get_job_list()
            current_status, task_id = self.get_status_from_list(job_list)
            if current_status:
                current_status = current_status.value
            status = TaskFutureStatus(
                job_type=self.job_type,
                cluster_status=prev_status.cluster_status,
                task_status=current_status or prev_status.task_status,
            )
            logger.warning(status)
        # task not submitted, query cluster status
        else:
            need_renew = STATUSCACHE.need_renew(self.cluster_name)
            cached_status = STATUSCACHE.get(self.cluster_name)
            logger.warning(f"cache {'miss' if need_renew else 'hit'} {self.cluster_name} | {cached_status}")
            if need_renew:
                STATUSCACHE.put(self.cluster_name, QueryCache(state=QueryState.QUERYING, last_result=getattr(prev_status, "last_result", None)))
                cluster_status = retry_handler(sky.status, [SkyErrorHandler(sqlite3.OperationalError, sqliteErrorHandler)])(
                    cluster_names=self.cluster_name
                )
                STATUSCACHE.put(self.cluster_name, QueryCache(state=QueryState.FINISHED, last_result=cluster_status))
            else:
                cluster_status = cached_status.last_result
            if not cluster_status:
                # starts putting but cluster haven't launched
                cluster_status = [{"status": sky.ClusterStatus(init_status.cluster_status)}]
            status = TaskFutureStatus(
                job_type=self.job_type,
                cluster_status=cluster_status[0]["status"].value,
                task_status=prev_status.task_status,
            )

        self.sky_task_id = task_id or self.sky_task_id
        self.to_proto_and_upload(status, self.remote_status_proto)
        return False
        
    async def put_task_status_loop(self, event_handler: EventHandler):
        init_status = TaskFutureStatus(job_type=self.job_type)
        self.to_proto_and_upload(init_status, self.remote_status_proto)
        # FIXME too long
        while True:
            # other coroutines cancelled / failed
            done = await self.put_task_status(event_handler)
            if done:
                return
            await asyncio.sleep(COROUTINE_INTERVAL)

    def handle_return(self, event_handler: EventHandler):
        # task submitted, need to cancel
        if event_handler.launch_done_event.is_set():
            self.delete_job()
        # task failed, put the failed status
        if event_handler.failed_event.is_set():
            task_status = self.get_task_status()
            task_status.task_status = LAUNCH_TYPE_TO_SKY_STATUS[task_status.job_type]("FAILED").value
            self.to_proto_and_upload(task_status, self.remote_status_proto)
        return


    async def delete_done(self, event_handler: EventHandler) -> bool:
        resource_meta = self.find_delete_proto()
        if resource_meta is not None:
            event_handler.cancel_event.set()
            self.handle_return(event_handler)
            return True
        if event_handler.is_terminal():
            return True
        return False

    async def deletion_status(self, event_handler: EventHandler):
        # checks if the task has been deleted from another pod
        while True:
            done = await self.delete_done(event_handler)
            if done:
                return
            await asyncio.sleep(COROUTINE_INTERVAL)


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
        self.home_sky_zip = os.path.join(self.local_sky_prefix, "home_sky.tar.gz")
        self.sky_key_zip = os.path.join(self.local_sky_prefix, "sky_key.tar.gz")
        self.home_sky_dir = os.path.join(self.local_sky_prefix, ".sky")
        self.home_key_dir = os.path.join(self.local_sky_prefix, ".ssh")

    def zip_sky_info(self):
        # compress ~/.sky to home_sky.tar.gz
        # sky_zip = shutil.make_archive(self.home_sky_zip, "gztar", os.path.expanduser("~/.sky"))
        with tarfile.open(self.home_sky_zip, "w:gz") as sky_tar:
            sky_tar.add(os.path.expanduser("~/.sky"), arcname=os.path.basename(".sky"))
        # tar ~/.ssh/sky-key* to sky_key.tar.gz
        local_key_dir = os.path.expanduser("~/.ssh")
        archived_keys = [file for file in os.listdir(local_key_dir) if file.startswith("sky-key")]
        with tarfile.open(self.sky_key_zip, "w:gz") as key_tar:
            for key_file in archived_keys:
                key_tar.add(os.path.join(local_key_dir, key_file), arcname=os.path.basename(key_file))
        return self.home_sky_zip


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
        bucket_name = file_provider.raw_output_fs._strip_protocol(file_provider.raw_output_prefix).split(
            file_provider.raw_output_fs.sep
        )[0]
        # s3://{bucket}/{SKY_DIRNAME}/{host_id}
        working_dir = file_provider.join(
            file_provider.raw_output_fs.unstrip_protocol(bucket_name),
            SKY_DIRNAME,
        )

        self.working_dir = file_provider.join(working_dir, self.unique_id)
        self.file_access = FileAccessProvider(local_sandbox_dir="/tmp", raw_output_prefix=self.working_dir)
        self.file_access.raw_output_fs.makedirs(self.working_dir, exist_ok=True)
        self.local_path_setting = LocalPathSetting(file_access=self.file_access, execution_id=self.unique_id)
        self.remote_path_setting = TrackerRemotePathSetting(file_access=self.file_access, unique_id=self.unique_id)

    async def zip_and_upload(self):
        # put ~/.ssh/sky-key* to sky_key.tar.gz
        logger.warning(self.local_path_setting)
        logger.warning(self.remote_path_setting)
        while True:
            sky_zip = self.local_path_setting.zip_sky_info()
            self.file_access.put_data(sky_zip, self.remote_path_setting.remote_sky_zip)
            self.file_access.put_data(self.local_path_setting.sky_key_zip, self.remote_path_setting.remote_key_zip)
            await asyncio.sleep(COROUTINE_INTERVAL)

    def download_and_unzip(self):
        local_sky_zip = os.path.join(
            os.path.dirname(self.local_path_setting.home_sky_zip), self.remote_path_setting._HOME_SKY
        )
        self.file_access.get_data(self.remote_path_setting.remote_sky_zip, local_sky_zip)
        self.file_access.get_data(self.remote_path_setting.remote_key_zip, self.local_path_setting.sky_key_zip)
        shutil.unpack_archive(local_sky_zip, self.local_path_setting.home_sky_dir)
        with tarfile.open(self.local_path_setting.sky_key_zip, "r:gz") as key_tar:
            key_tar.extractall(self.local_path_setting.home_key_dir)
        return

    def last_upload_time(self) -> Optional[datetime]:
        if self.file_access.exists(self.remote_path_setting.remote_sky_zip):
            self.file_access.raw_output_fs.ls(self.remote_path_setting.remote_sky_zip, refresh=True)
            return self.file_access.raw_output_fs.modified(self.remote_path_setting.remote_sky_zip)


def setup_cloud_credential(show_check: bool = False):
    if show_check:
        sky.check.check()
    return


def parse_blob_basename(blob_name: str) -> str:
    """
    Parse the blob name to get the task_id
    """
    file_access_provider = FileAccessProvider(local_sandbox_dir="/tmp", raw_output_prefix=blob_name)
    file_system = file_access_provider.raw_output_fs
    blob_name = blob_name.strip(file_system.sep).split(file_system.sep)[-1]
    return blob_name


class TaskCreationIdentifier(object):
    _output_prefix: str
    _task_id: str
    def __init__(self, output_prefix: str, task_execution_metadata: TaskExecutionMetadata) -> None:
        self._output_prefix = output_prefix
        # local execution
        if task_execution_metadata is None:
            task_id = parse_blob_basename(output_prefix)
            self._task_id = task_id
        else:
            task_id = task_execution_metadata.task_execution_id.node_execution_id.execution_id.name
            retry_suffix = 0 if task_execution_metadata.task_execution_id.retry_attempt is None else task_execution_metadata.task_execution_id.retry_attempt
            task_id = f"{task_id}-{retry_suffix}"
            self._task_id = task_id
            
    @property
    def task_id(self):
        return self._task_id
    
    @property
    def output_prefix(self):
        return self._output_prefix
    
    



def sqliteErrorHandler():
    # reinitialize the db connection
    logger.warning("Reinitializing the db connection")
    db_path = sky.global_user_state._DB.db_path
    sky.global_user_state._DB.conn.close()
    sky.global_user_state._DB = sky.utils.db_utils.SQLiteConn(
        db_path, sky.global_user_state.create_table
    )
    
    
class SkyErrorHandler:
    def __init__(self, error: Exception, handler: Callable, return_on_handled: bool = False) -> None:
        self.error = error
        self.handler = handler
        self.return_on_handled = return_on_handled
        
    def handle(self, e: Exception) -> bool:
        if isinstance(e, self.error):
            self.handler()
            return True
        return False
    
    
def retry_handler(fn, errors: List[SkyErrorHandler], max_retries: int = 3):
    def decorator(*args, **kwargs):
        for i in range(max_retries):
            try:
                return fn(*args, **kwargs)
            except Exception as e:
                handled = False
                for error in errors:
                    handled = handled or error.handle(e)
                    if handled and error.return_on_handled:
                        return None

                if i == max_retries - 1 or not handled:
                    raise e

    return decorator

class BaseSkyTask:
    def __init__(
        self, 
        task: sky.Task, 
        path_setting: TaskRemotePathSetting, 
        cluster_event_handler: EventHandler, 
        queue_cache: QueryCache) -> None:
        self._task = task
        self._sky_path_setting = path_setting
        self._cluster_event_handler = cluster_event_handler
        self._task_event_handler = EventHandler()
        self._queue_cache = queue_cache
        self._task_status = TaskStatus.INIT
        self._backend = sky.backends.CloudVmRayBackend()
        
    def run_task(self):
        raise NotImplementedError
        
        
    @property
    def task_event_handler(self):
        return self._task_event_handler
class NormalTask(BaseSkyTask):
    
    def __init__(
        self,
        task: sky.Task,
        path_setting: TaskRemotePathSetting,
        cluster_event_handler: EventHandler,
        queue_cache: QueryCache,
    ) -> None:
        super().__init__(task, path_setting, cluster_event_handler, queue_cache)
    
    def run_task(self):
        init_status = TaskFutureStatus(job_type=self._sky_path_setting.job_type)
        self._sky_path_setting.to_proto_and_upload(init_status, self._sky_path_setting.remote_status_proto)
        self._create_task_coroutine = asyncio.create_task(self.start())
        self._create_task_coroutine.add_done_callback(self.launch_done_callback)
    
    async def start(self):
        while not self._cluster_event_handler.launch_done_event.is_set():
            deleted = await self._sky_path_setting.delete_done(self._task_event_handler)
            if deleted:
                return
            await asyncio.sleep(COROUTINE_INTERVAL)
        self._task_status = TaskStatus.CLUSTER_UP
        self._launch_fn_handler = BlockingProcessHandler(functools.partial(self.launch, task=self._task))
        await self._launch_fn_handler.status_poller(self._task_event_handler)
        self._task_status = TaskStatus.TASK_SUBMITTED
        if self._task_event_handler.launch_done_event.is_set():
            while not self._task_event_handler.is_terminal():
                await self._sky_path_setting.delete_done(self._task_event_handler)
                job_queue = await self.get_job_list()
                await self._sky_path_setting.put_task_status(self._task_event_handler, job_queue)
                await asyncio.sleep(COROUTINE_INTERVAL)
            self._task_status = TaskStatus.DONE

    async def get_job_list(self) -> List[Dict[str, Any]]:
        queue_key = self._sky_path_setting.cluster_name if self._sky_path_setting.job_type == JobLaunchType.NORMAL else "$job$"
        need_renew = self._queue_cache.need_renew()
        prev_queue = self._queue_cache.last_result
        logger.warning(f"cache {'miss' if need_renew else 'hit'} {queue_key} | {prev_queue}")
        if not need_renew:
            return prev_queue
        self._queue_cache.update(state=QueryState.QUERYING, last_result=prev_queue)
        queue_result = None
        queue_retry_handler = retry_handler(
            sky.queue, 
            [SkyErrorHandler(sqlite3.OperationalError, sqliteErrorHandler), SkyErrorHandler(sky.exceptions.ClusterNotUpError, lambda :None, True)]
        )
        try:
            async with asyncio.timeout(QUEUE_TIMEOUT):
                queue_result = await asyncio.to_thread(queue_retry_handler, self._sky_path_setting.cluster_name)            
        except asyncio.TimeoutError:
            raise asyncio.TimeoutError(f"Querying queue for {self._sky_path_setting.cluster_name} timed out.")
        self._queue_cache.update(state=QueryState.FINISHED, last_result=queue_result)
        return queue_result or []

    def launch_done_callback(self, task: asyncio.Task, clean_ups: List[Callable] = None):
        cause = "exec task"
        if clean_ups is None:
            clean_ups = []
        error_cause = None
        try:
            # check if is cancelled/done/exception
            if task.exception() is not None:
                try:
                    error_cause = "Exception"
                    task.result()
                except Exception:
                    # re-raise the exception
                    error = traceback.format_exc()
                    self._sky_path_setting.put_error_log(error)
                    logger.error(
                        f"Cluster {self._sky_path_setting.cluster_name} failed to {cause}.\n"
                        f"Cause: \n{error}\n"
                        # f"error_log is at {self.sky_path_setting.remote_error_log}"
                    )

                self._task_event_handler.failed_event.set()
            else:
                # normal exit, if this marks the end of all coroutines, cancel them all
                self._task_event_handler.finished_event.set()
        except asyncio.exceptions.CancelledError:
            self._task_event_handler.cancel_event.set()
            error_cause = "Cancelled"
        finally:
            logger.warning(f"{cause} coroutine finished with {error_cause}.")
            # if self._task_event_handler.is_terminal() and self._launch_fn_handler._process.exitcode is not None:
                # pass
            if self._task_status == TaskStatus.TASK_SUBMITTED and self._task_event_handler.is_terminal():
                self._task_status = TaskStatus.DONE
                if not self._task_event_handler.finished_event.is_set():
                    sky.cancel(
                        cluster_name=self._sky_path_setting.cluster_name, job_ids=[self._sky_path_setting.sky_task_id]
                    )

    def launch(self, task: sky.Task):
        sky.exec(
            task=task,
            cluster_name=self._sky_path_setting.cluster_name,
            stream_logs=False,
            backend=self._backend,
            detach_run=True
        )
        

        
        
class ClusterManager(object):
    def __init__(self, cluster_name: str, cluster_type: JobLaunchType) -> None:
        self._cluster_name = cluster_name
        self._cluster_type = cluster_type
        self._query_cache = QueryCache()
        self._tasks: Dict[str, BaseSkyTask] = {}
        
    def submit(self, task: sky.Task):
        if self._cluster_type == JobLaunchType.NORMAL:
            return sky.submit(cluster_name=self._cluster_name, task=task)
        return sky.jobs.submit(name=task.name, task=task)

    def launch(self, task: sky.Task, last_status: List[Dict[str, Any]] = None):
        raise NotImplementedError
    
    @property
    def tasks(self):
        return self._tasks

class NormalClusterManager(ClusterManager):
    def __init__(self, cluster_name: str, cluster_type: JobLaunchType) -> None:
        super().__init__(cluster_name, cluster_type)
        self._backend = sky.backends.CloudVmRayBackend()
        self._cluster_launcher = None
        self._cluster_event_handler = None
        self._cluster_coroutine = None
        
    def reset(self):
        self._tasks.clear()
        self._cluster_launcher = None
        self._cluster_event_handler = None
        self._cluster_coroutine = None
        
    def submit(self, task: sky.Task, sky_path_setting: TaskRemotePathSetting):
        if self.check_all_done():
            self.reset()
            logger.warning(f"Cluster {self._cluster_name} tear down, restarting cluster")
            self._cluster_event_handler = ClusterEventHandler()
            self._cluster_coroutine = asyncio.create_task(self.start_cluster_life_cycle(task, True))
            self._cluster_coroutine.add_done_callback(self.cluster_up_callback)
        self._tasks[task.name] = NormalTask(task, sky_path_setting, self._cluster_event_handler._cluster_handler, self._query_cache)
        self._cluster_event_handler.register_task_handler(self._tasks[task.name].task_event_handler)
        self._tasks[task.name].run_task()
        
    def cluster_up_callback(self, task: asyncio.Task, clean_ups: List[Callable] = None):
        cause = "cluster up"
        if clean_ups is None:
            clean_ups = []
        error_cause = None
        try:
            # check if is cancelled/done/exception
            if task.exception() is not None:
                try:
                    error_cause = "Exception"
                    task.result()
                except Exception:
                    # re-raise the exception
                    error = traceback.format_exc()
                    # self.sky_path_setting.put_error_log(error)
                    logger.error(
                        f"Cluster {self._cluster_name} failed to {cause}.\n"
                        f"Cause: \n{error}\n"
                        # f"error_log is at {self.sky_path_setting.remote_error_log}"
                    )

                self._cluster_event_handler.failed_event.set()
            else:
                # normal exit, if this marks the end of all coroutines, cancel them all
                self._cluster_event_handler.finished_event.set()
        except asyncio.exceptions.CancelledError:
            self._cluster_event_handler.cancel_event.set()
            error_cause = "Cancelled"
        finally:
            logger.warning(f"{cause} coroutine finished with {error_cause}.")
            if self._cluster_event_handler.is_terminal():
                # on first entry to done_callback, stop the clusters
                sky.stop(self._cluster_name)
        
    async def start_cluster_life_cycle(self, task, down: bool):
        setup_cloud_credential(show_check=True)
        if down:
            try:
                sky.down(self._cluster_name)
                logger.warning(f"Cluster {self._cluster_name} is down.")
            except ValueError:
                pass
        
        self._cluster_launcher = BlockingProcessHandler(functools.partial(self.launch_dummy_task, task=task))
        await self._cluster_launcher.status_poller(self._cluster_event_handler)
        while True:
            all_done = self.check_all_done()
            if all_done:
                return
            await asyncio.sleep(COROUTINE_INTERVAL)
        
        # TODO: Check if the all tasks in cluster is done
    def check_all_done(self):
        for task in self._tasks.values():
            if not task._task_event_handler.is_terminal():
                return False
        return True

    def launch_dummy_task(self, task: sky.Task):
        dummy_task = task.to_yaml_config()
        dummy_task.pop('run', None)
        task_name = dummy_task.get('name', None)
        dummy_task.update({'name': f"{task_name}-dummy"})
        dummy_task = sky.Task.from_yaml_config(dummy_task)
        self._cluster_launcher = sky.launch(
            task=dummy_task, 
            cluster_name=self._cluster_name, 
            backend=self._backend,
            stream_logs=False,
            detach_setup=True,
            detach_run=True
        )
        

class ManagedClusterManager(ClusterManager):
    def __init__(self, cluster_name: str, cluster_type: JobLaunchType) -> None:
        super().__init__(cluster_name, cluster_type)
        
    def launch(self, task: sky.Task, last_status: List[Dict[str, Any]] = None):
        sky.jobs.launch(task=task, detach_run=True, stream_logs=False)


class ClusterRegistry:
    def __init__(self) -> None:
        self._clusters: Dict[str, ClusterManager] = {}
        
        
    def create(self, task: sky.Task, sky_path_setting: TaskRemotePathSetting):
        cluster_name = sky_path_setting.cluster_name
        cluster_type = sky_path_setting.job_type
        if cluster_type == JobLaunchType.NORMAL:
            manager = self._clusters.get(cluster_name, NormalClusterManager(cluster_name, cluster_type))
            self._clusters[cluster_name] = manager
        else:
            manager = self._clusters.get(cluster_name, ManagedClusterManager(cluster_name, cluster_type))
            self._clusters[cluster_name] = manager
            
        manager.submit(task, sky_path_setting)
        
        
    def get(self, resource_meta: SkyPilotMetadata) -> bool:
        manager = self._clusters.get(resource_meta.cluster_name, None)
        if not manager:
            return False
        task = manager.tasks.get(resource_meta.job_name, None)
        if not task:
            return False
        return True
        
        
            