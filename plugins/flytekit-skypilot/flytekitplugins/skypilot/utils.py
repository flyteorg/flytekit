import asyncio
import enum
import functools
import os
import pathlib
import shutil
import tarfile
import tempfile
import traceback
from dataclasses import asdict, dataclass, field
from datetime import datetime
from typing import Any, Callable, Dict, List, Optional, Tuple, Union

import sky
import sky.exceptions
import sky.jobs
from flyteidl.core.execution_pb2 import TaskExecution
from flytekitplugins.skypilot.constants import (
    AUTO_DOWN,
    COROUTINE_INTERVAL,
    DOWN_TIMEOUT,
    MINIMUM_SLEEP,
    SKY_DIRNAME,
)
from flytekitplugins.skypilot.metadata import JobLaunchType, SkyPilotMetadata
from flytekitplugins.skypilot.process_utils import (
    BaseProcessHandler,
    BlockingProcessHandler,
    ClusterEventHandler,
    ConcurrentProcessHandler,
    EventHandler,
    SkySubProcessError,
    manage_subtasks,
)
from google.protobuf.json_format import MessageToDict
from google.protobuf.struct_pb2 import Struct
from pydantic import BaseModel

from flytekit import logger
from flytekit.core.data_persistence import FileAccessProvider
from flytekit.models.task import TaskExecutionMetadata

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


class QueryState(enum.Enum):
    QUERYING = enum.auto()
    FINISHED = enum.auto()


class QueryCache:
    """
    Because we don't want to busy query the same cluster multiple times, we use this cache to store the query result.
    """

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


class ClusterStatus(int, enum.Enum):
    DOWN = enum.auto()  # new cluster
    INIT = enum.auto()  # cluster is initializing
    DUMMY_LAUNCHING = enum.auto()  # cluster is launching dummy task
    FAILED = enum.auto()  # cluster failed to dummy launch
    UP = enum.auto()  # cluster is up
    WAIT_FOR_TASKS = enum.auto()
    STOPPING = enum.auto()  # cluster is stopping


def get_sky_autostop(cluster_name: str, idle_minutes: int, down: bool = False) -> Callable:
    return functools.partial(sky.autostop, cluster_name, idle_minutes, down=down)


def sky_dummy_with_auto_stop(task: sky.Task, cluster_name: str, backend: sky.backends.CloudVmRayBackend):
    try:
        get_sky_autostop(cluster_name, -1)()
    except (ValueError, sky.exceptions.ClusterNotUpError):
        pass
    sky.launch(
        task=task,
        cluster_name=cluster_name,
        backend=backend,
        stream_logs=False,
        detach_setup=True,
        detach_run=False,
        idle_minutes_to_autostop=AUTO_DOWN / 60,
        down=True,
    )


class MinimalProvider(BaseModel):
    """
    pydatnic model for `FileAccessProvider`
    """

    local_sandbox_dir: str
    raw_output_prefix: str


class MinimalRemoteSetting(BaseModel):
    """
    pydatnic model for `TaskRemotePathSetting`
    """

    file_access: MinimalProvider
    unique_id: str
    job_type: JobLaunchType
    cluster_name: str
    sky_task_id: Optional[int] = None
    task_name: Optional[str] = None

    class Config:
        extra = "forbid"  # Prevents additional fields
        use_enum_values = True  # Serializes enums to their values


@dataclass
class BaseRemotePathSetting:
    file_access: FileAccessProvider
    remote_sky_dir: str = field(init=False)

    def __post_init__(self):
        self.remote_sky_dir = self.file_access.join(self.file_access.raw_output_prefix, ".skys")
        self.file_access.raw_output_fs.makedirs(self.remote_sky_dir, exist_ok=True)

    def remote_exists(self):
        """
        deprecated
        """
        raise NotImplementedError

    def remote_failed(self):
        raise NotImplementedError

    def find_delete_proto(self) -> Dict[str, Any]:
        raise NotImplementedError

    def to_minimal_setting(self):
        raise NotImplementedError


@dataclass
class TrackerRemotePathSetting(BaseRemotePathSetting):
    """
    deprecated, originally used to preserve sky key per replica, but we now use PVC
    """

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

    def task_exists(self, task_id: str):
        """
        deprecated
        """
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
    """
    manages the remote path for the task
    """

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
            unique_id=self.unique_id,
        )

    def __post_init__(self):
        super().__post_init__()
        self.remote_error_log = self.file_access.join(self.remote_sky_dir, "error.log")
        self.remote_delete_proto = self.file_access.join(self.remote_sky_dir, "delete.pb")
        self.remote_status_proto = self.file_access.join(self.remote_sky_dir, "status.pb")
        # logger.warning(self)

    def remote_failed(self):
        return self.file_access.exists(self.remote_error_log)

    def find_delete_proto(self) -> Optional[SkyPilotMetadata]:
        """
        check if the delete proto exists, if so, return the metadata
        """
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
        error_str = self.get_error_log()
        if error_str is not None:
            logger.error(f"Error log found: {error_str}")
            return TaskFutureStatus(status.job_type, status.cluster_status, sky.JobStatus.FAILED.value)
        return status

    def get_error_log(self) -> Optional[str]:
        error_str = None
        if self.remote_failed():
            local_log_file = self.file_access.get_random_local_path()
            self.file_access.get_data(self.remote_error_log, local_log_file)
            with open(local_log_file, "r") as f:
                error_str = f.read()
            os.remove(local_log_file)
        return error_str

    def get_status_from_list(self, job_list) -> Tuple[sky.JobStatus, Optional[int]]:
        for job in job_list:
            if job["job_name"] == self.task_name:
                return job["status"], job["job_id"]
        return None, None

    async def put_task_status(self, event_handler: EventHandler, job_list: List[Dict[str, Any]]) -> bool:
        if self.remote_failed():
            event_handler.failed_event.set()

        if event_handler.is_terminal():
            self.handle_return(event_handler)
            return True
        task_id = None
        prev_status = self.get_task_status()
        status = prev_status
        logger.warning(event_handler)
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
        # task finished
        if LAUNCH_TYPE_TO_SKY_STATUS[self.job_type](status.task_status).is_terminal():
            event_handler.finished_event.set()
            return True
        return False

    def handle_return(self, event_handler: EventHandler):
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
        """
        busy check the deletion status
        """
        while True:
            done = await self.delete_done(event_handler)
            if done:
                return
            await asyncio.sleep(COROUTINE_INTERVAL)

    def to_minimal_setting(self) -> MinimalRemoteSetting:
        return MinimalRemoteSetting(
            file_access=MinimalProvider(
                local_sandbox_dir=str(self.file_access.local_sandbox_dir),
                raw_output_prefix=self.file_access.raw_output_prefix,
            ),
            unique_id=self.unique_id,
            job_type=self.job_type,
            cluster_name=self.cluster_name,
            sky_task_id=self.sky_task_id,
            task_name=self.task_name,
        )

    @classmethod
    def from_minimal_setting(cls, setting: MinimalRemoteSetting):
        return cls(
            file_access=FileAccessProvider(
                local_sandbox_dir=setting.file_access.local_sandbox_dir,
                raw_output_prefix=setting.file_access.raw_output_prefix,
            ),
            unique_id=setting.unique_id,
            job_type=setting.job_type,
            cluster_name=setting.cluster_name,
            sky_task_id=setting.sky_task_id,
            task_name=setting.task_name,
        )


@dataclass
class LocalPathSetting:
    """
    deprecated, originally used to preserve sky key per replica, but we now use PVC
    """

    file_access: FileAccessProvider
    execution_id: str
    local_sky_prefix: str = None
    home_sky_zip: str = None
    sky_key_zip: str = None
    home_sky_dir: str = None
    home_key_dir: str = None

    def __post_init__(self):
        self.local_sky_prefix = os.path.join(self.file_access.local_sandbox_dir, self.execution_id, ".skys")
        pathlib.Path(self.local_sky_prefix).mkdir(parents=True, exist_ok=True)
        self.home_sky_zip = os.path.join(self.local_sky_prefix, "home_sky.tar.gz")
        self.sky_key_zip = os.path.join(self.local_sky_prefix, "sky_key.tar.gz")
        self.home_sky_dir = os.path.join(self.local_sky_prefix, ".sky")
        self.home_key_dir = os.path.join(self.local_sky_prefix, ".ssh")

    def zip_sky_info(self):
        # compress ~/.sky to home_sky.tar.gz
        # TODO: this is not atomic, need to fix
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
    """
    deprecated, originally used to preserve sky key per replica, but we now use PVC
    """

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
            self.zip_and_put()
            await asyncio.sleep(COROUTINE_INTERVAL)

    def zip_and_put(self):
        sky_zip = self.local_path_setting.zip_sky_info()
        self.file_access.put_data(sky_zip, self.remote_path_setting.remote_sky_zip)
        self.file_access.put_data(self.local_path_setting.sky_key_zip, self.remote_path_setting.remote_key_zip)

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
        return None


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
            node_id = task_execution_metadata.task_execution_id.node_execution_id.node_id
            retry_suffix = (
                0
                if task_execution_metadata.task_execution_id.retry_attempt is None
                else task_execution_metadata.task_execution_id.retry_attempt
            )
            task_id = f"{task_id}-{node_id}-{retry_suffix}"
            self._task_id = task_id

    @property
    def task_id(self):
        return self._task_id

    @property
    def output_prefix(self):
        return self._output_prefix


def sqliteErrorHandler():
    """
    deprecated
    """
    # reinitialize the db connection
    logger.warning("Reinitializing the db connection")
    db_path = sky.global_user_state._DB.db_path
    sky.global_user_state._DB.conn.close()
    sky.global_user_state._DB = sky.utils.db_utils.SQLiteConn(db_path, sky.global_user_state.create_table)


class SkyErrorHandler:
    """
    deprecated
    """

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
    """
    deprecated
    """

    @functools.wraps(fn)
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
        queue_cache: QueryCache,
    ) -> None:
        self._task = task
        logger.warning(path_setting)
        self._sky_path_setting = path_setting
        self._cluster_event_handler = cluster_event_handler
        self._task_event_handler = EventHandler()
        self._queue_cache = queue_cache
        self._task_status = TaskStatus.INIT
        self._backend = sky.backends.CloudVmRayBackend()
        self._create_task_coroutine: Optional[asyncio.Task] = None
        self._cancel_check_corotine: Optional[asyncio.Task] = None

    def run_task(self):
        init_status = TaskFutureStatus(job_type=self._sky_path_setting.job_type)
        self._sky_path_setting.to_proto_and_upload(init_status, self._sky_path_setting.remote_status_proto)
        self._create_task_coroutine = asyncio.create_task(self.start())
        self._create_task_coroutine.add_done_callback(self.launch_done_callback)

    @property
    def task_event_handler(self):
        return self._task_event_handler

    @property
    def cluster_name(self):
        return self._sky_path_setting.cluster_name

    def cancel_task(self):
        raise NotImplementedError

    def get_cancel_handler(self) -> Callable:
        raise NotImplementedError

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
                except Exception as e:
                    # re-raise the exception
                    self._sky_path_setting.put_error_log(e)
                    logger.error(
                        f"Task {self._sky_path_setting.task_name} failed to {cause}.\n"
                        f"Cause: \n{e}\n"
                        f"error_log is at {self._sky_path_setting.remote_error_log}"
                    )
                finally:
                    self._task_event_handler.failed_event.set()
            else:
                # normal exit
                self._task_event_handler.finished_event.set()
        except asyncio.exceptions.CancelledError:
            self._task_event_handler.cancel_event.set()
            error_cause = "Cancelled"
        finally:
            logger.warning(f"{cause} coroutine finished with {error_cause}.")
            if self._task_event_handler.failed_event.is_set():
                # make sure agent knows the task failed
                self._sky_path_setting.handle_return(self._task_event_handler)

    def get_queue_handler(self) -> Callable:
        raise NotImplementedError

    def get_launch_handler(self, task: sky.Task) -> Callable:
        raise NotImplementedError

    async def get_job_list(self) -> List[Dict[str, Any]]:
        # prevent multiple tasks querying the same cluster
        need_renew = self._queue_cache.need_renew()
        prev_queue = self._queue_cache.last_result
        if not need_renew:
            return prev_queue
        self._queue_cache.update(state=QueryState.QUERYING, last_result=prev_queue)
        queue_result = None
        queue_retry_handler = self.get_queue_handler()
        queue_handler = ConcurrentProcessHandler(
            functools.partial(sky_error_reraiser, queue_retry_handler),
            self._task_event_handler,
            name=f"sky {self.cluster_name} queue",
        )
        success, queue_result = await queue_handler.status_poller()
        self._queue_cache.update(state=QueryState.FINISHED, last_result=queue_result)
        return queue_result or []

    def launch(self, task: sky.Task):
        raise NotImplementedError

    async def on_init(self) -> bool:
        """
        waiting before cluster comes up, returns false if task is cancelled or cluster launch failed
        """
        async with manage_subtasks() as manager:
            manager.create_task(self._cluster_event_handler.launch_done_event.wait())
            manager.create_task(self._cluster_event_handler.failed_event.wait())
            manager.create_task(self._task_event_handler.cancel_event.wait())
            done, pending = await manager.wait_first_done()

        if self._cluster_event_handler.failed_event.is_set():
            self._task_event_handler.failed_event.set()
            return False

        if self._task_event_handler.cancel_event.is_set():
            return False
        return self._cluster_event_handler.launch_done_event.is_set()

    async def start(self):
        # register cancel checker
        self._cancel_check_corotine = asyncio.create_task(
            self._sky_path_setting.deletion_status(self._task_event_handler)
        )
        # stage 0: waiting cluster to up. fail cause: cluster launch failed, cancelled | ClusterStatus.DOWN - ClusterStatus.UP
        logger.warning(f"Task {self._sky_path_setting.task_name} is in {self._task_status} status.")
        init = await self.on_init()
        if not init:
            return
        self._task_status = TaskStatus.CLUSTER_UP
        # stage 1: launch task, fail cause: canceled, job submit failed | ClusterStatus.UP -
        logger.warning(f"Task {self._sky_path_setting.task_name} is in {self._task_status} status.")
        self._launch_fn_handler = ConcurrentProcessHandler(
            functools.partial(sky_error_reraiser, self.get_launch_handler(self._task)),
            event_handler=self._task_event_handler,
            name=f"task {self._sky_path_setting.task_name}",
        )
        launched, launch_result = await self._launch_fn_handler.status_poller()
        if not launched:
            return
        job_id, _ = launch_result if launch_result is not None else (None, None)
        self._sky_path_setting.sky_task_id = job_id or self._sky_path_setting.sky_task_id
        self._task_status = TaskStatus.TASK_SUBMITTED
        # stage 2: waiting task to finish | ClusterStatus.UP -
        logger.warning(f"task {self._sky_path_setting.task_name}: #{job_id} is in {self._task_status} status.")
        self._cancel_check_corotine.cancel()
        if self._task_event_handler.launch_done_event.is_set():
            while not self._task_event_handler.is_terminal():
                # cases: cancelled (handled internally), failed
                await self._sky_path_setting.delete_done(self._task_event_handler)
                # cases: failed to get job queue
                job_queue = await self.get_job_list()
                # cases: failed to put task status
                await self._sky_path_setting.put_task_status(self._task_event_handler, job_queue)
                await asyncio.sleep(COROUTINE_INTERVAL)

            if self._task_event_handler.cancel_event.is_set():
                self._task_status = TaskStatus.DONE
                if not self._task_event_handler.finished_event.is_set():
                    cancel_process_handler = BlockingProcessHandler(
                        functools.partial(sky_error_reraiser, self.cancel_task),
                        name=f"cancel {self._sky_path_setting.task_name}",
                    )
                    await cancel_process_handler.status_poller()
            self._task_status = TaskStatus.DONE

    def put_error_log(self, error_log: Exception):
        self._sky_path_setting.put_error_log(error_log)

    @property
    def task_done(self):
        """
        either
        #### 1. task is finished
        #### 2. task is terminal(failed/cancelled) and hasn't submitted
        """
        return self._task_event_handler.finished_event.is_set() or (
            self._task_event_handler.is_terminal() and self._task_status < TaskStatus.TASK_SUBMITTED
        )

    def stop_all_coroutines(self):
        if self._create_task_coroutine and not self._create_task_coroutine.done():
            self._create_task_coroutine.cancel()
        if self._cancel_check_corotine and not self._cancel_check_corotine.done():
            self._cancel_check_corotine.cancel()


class NormalTask(BaseSkyTask):
    """
    tasks that use normal sky job
    """

    def __init__(
        self,
        task: sky.Task,
        path_setting: TaskRemotePathSetting,
        cluster_event_handler: EventHandler,
        queue_cache: QueryCache,
    ) -> None:
        super().__init__(task, path_setting, cluster_event_handler, queue_cache)

    def cancel_task(self):
        assert self._sky_path_setting.sky_task_id is not None
        sky.cancel(cluster_name=self.cluster_name, job_ids=[self._sky_path_setting.sky_task_id])

    def get_cancel_handler(self) -> Callable:
        return functools.partial(sky.cancel, cluster_name=self.cluster_name)

    def get_queue_handler(self) -> Callable:
        return functools.partial(sky.queue, cluster_name=self.cluster_name)

    def get_launch_handler(self, task: sky.Task) -> Callable:
        return functools.partial(
            sky.exec,
            task=task,
            cluster_name=self.cluster_name,
            stream_logs=False,
            backend=self._backend,
            detach_run=True,
        )

    def launch(self, task: sky.Task):
        sky.exec(
            task=task,
            cluster_name=self.cluster_name,
            stream_logs=False,
            backend=self._backend,
            detach_run=True,
        )


class ManagedTask(BaseSkyTask):
    """
    tasks that use managed sky job
    """

    def __init__(
        self,
        task: sky.Task,
        path_setting: TaskRemotePathSetting,
        cluster_event_handler: EventHandler,
        queue_cache: QueryCache,
    ) -> None:
        super().__init__(task, path_setting, cluster_event_handler, queue_cache)

    def get_queue_handler(self) -> Callable:
        return functools.partial(sky.jobs.queue, True)

    def get_launch_handler(self, task: sky.Task) -> Callable:
        return functools.partial(sky.jobs.launch, task=task, detach_run=False, stream_logs=False)

    def cancel_task(self):
        sky.jobs.cancel(name=self._sky_path_setting.task_name)

    def get_cancel_handler(self) -> Callable:
        return functools.partial(sky.jobs.cancel, name=self._sky_path_setting.task_name)

    def launch(self, task: sky.Task):
        sky.jobs.launch(task=task, detach_run=True, stream_logs=False)


class ClusterManager(object):
    def __init__(self, cluster_name: str, cluster_type: JobLaunchType) -> None:
        """
        DOWN -> INIT -> DUMMY_LAUNCHING -> UP ->(after cluster autodown) DUMMY_LAUNCHING\n
        DOWN -> INIT -> DUMMY_LAUNCHING(failed/all cancelled) -> STOPPING
        """
        self._cluster_name: str = cluster_name
        self._cluster_type: JobLaunchType = cluster_type
        self._query_cache: QueryCache = QueryCache()
        self._tasks: Dict[str, BaseSkyTask] = {}
        self._cluster_status = ClusterStatus.DOWN
        self._cluster_stop_handler: Optional[BaseProcessHandler] = None
        self._cluster_launcher: Optional[BaseProcessHandler] = None
        self._cluster_coroutine: Optional[asyncio.Task] = None
        self._cluster_event_handler = ClusterEventHandler()

    def launch(self, task: sky.Task, last_status: List[Dict[str, Any]] = None):
        raise NotImplementedError

    @property
    def tasks(self):
        return self._tasks

    async def reset(self):
        self._tasks = {k: v for k, v in self._tasks.items() if not v.task_done}
        self._cluster_status = (
            ClusterStatus.INIT if self._cluster_status == ClusterStatus.DOWN else self._cluster_status
        )
        self._cluster_launcher = None
        self._cluster_coroutine = None
        # cluster is stopping, cancel the task
        if self._cluster_stop_handler and not self._cluster_stop_handler.done():
            await self._cluster_stop_handler.cancel()
            logger.warning(f"Cluster {self._cluster_name} stop coroutine cancelled.")
            await asyncio.sleep(MINIMUM_SLEEP)
        self._cluster_stop_handler = None
        self._cluster_event_handler = ClusterEventHandler()
        self._query_cache = QueryCache()

    def create_task(
        self,
        task: sky.Task,
        sky_path_setting: TaskRemotePathSetting,
        event_handler: EventHandler,
        queue_cache: QueryCache,
    ) -> BaseSkyTask:
        raise NotImplementedError

    async def submit(self, task: sky.Task, sky_path_setting: TaskRemotePathSetting):
        start_cluster = False
        if (
            self._cluster_coroutine is None or self._cluster_coroutine.done()
        ) and self._cluster_status != ClusterStatus.INIT:
            # cluster coro is None -> new cluster
            # cluster coro is done -> either STOPPING or UP
            await self.reset()
            logger.warning(f"Cluster {self._cluster_name} torn down, restarting cluster")
            start_cluster = True

        logger.debug(f"Cluster {self._cluster_name} start task {task} creation")
        logger.debug(f"Task setup: {task.setup}")
        new_task = self.create_task(task, sky_path_setting, self._cluster_event_handler, self._query_cache)
        self._tasks[task.name] = new_task
        self._cluster_event_handler.register_task_handler(self._tasks[task.name].task_event_handler)
        self._tasks[task.name].run_task()
        logger.warning(f"Cluster {self._cluster_name} start task run")
        if start_cluster:
            self._cluster_coroutine = asyncio.create_task(self.start_cluster_life_cycle(task))
            self._cluster_coroutine.add_done_callback(self.cluster_up_callback)
            logger.warning(f"Cluster {self._cluster_name} start cluster lifecycle")
        await asyncio.sleep(MINIMUM_SLEEP)

    async def cluster_inspector(self) -> bool:
        logger.warning(f"Inspecting: Cluster {self._cluster_name} is {self._cluster_status}.")
        if self._cluster_status == ClusterStatus.UP:
            # if cluster is UP, maybe it's autostopped, we need to check that
            autostop_process = ConcurrentProcessHandler(
                functools.partial(
                    sky_error_reraiser, functools.partial(sky.autostop, self._cluster_name, AUTO_DOWN // 60, down=True)
                ),
                name=f"autostop {self._cluster_name}",
            )
            try:
                await autostop_process.status_poller()
                return False
            except SkySubProcessError:
                logger.error(f"Failed to autostop {self._cluster_name}.")
                return True

        if self._cluster_status == ClusterStatus.DUMMY_LAUNCHING:
            return False

        if self._cluster_status == ClusterStatus.STOPPING:
            return True

        if self._cluster_status == ClusterStatus.FAILED:
            return True

        return True

    def cluster_up_callback(self, task: asyncio.Task):
        cause = "cluster up"
        error_cause = None
        try:
            # check if is cancelled/done/exception
            if task.exception() is not None:
                put_error = None
                try:
                    error_cause = "Exception"
                    task.result()
                except Exception as e:
                    # re-raise the exception
                    logger.error(f"Cluster {self._cluster_name} failed to {cause}.\n" f"Cause: \n{e}\n")
                    put_error = e
                finally:
                    # failed to launch cluster
                    self._cluster_status = ClusterStatus.FAILED
                    self._cluster_event_handler.failed_event.set()
                    for _task in self._tasks.values():
                        _task._task_event_handler.failed_event.set()
                        assert put_error is not None
                        _task.put_error_log(put_error)
            else:
                # normal exit
                self._cluster_event_handler.finished_event.set()
        # handle re-cancel
        except asyncio.exceptions.CancelledError:
            self._cluster_event_handler.cancel_event.set()
            error_cause = "Cancelled"
        finally:
            logger.warning(f"{self._cluster_name} {cause} coroutine finished with {error_cause}.")
            self.stop_cluster()

    async def start_cluster_life_cycle(self, task):
        """
        invoked when cluster is stopped or down
        """
        raise NotImplementedError

    def check_all_done(self):
        for task in self._tasks.values():
            if not task.task_done:
                return False
        return True

    def stop_cluster(self):
        return

    async def wait_all_tasks(self):
        """
        deprecated, periodically check if all tasks are done
        """
        self._cluster_status = ClusterStatus.WAIT_FOR_TASKS
        logger.warning(f"Cluster {self._cluster_name} is {self._cluster_status}.")
        done_time_counter = 0
        while True:
            await asyncio.sleep(COROUTINE_INTERVAL)
            all_done = self.check_all_done()
            if all_done:
                logger.warning(f"Cluster {self._cluster_name} is {self._cluster_status} with {done_time_counter}.")
                done_time_counter += COROUTINE_INTERVAL
            else:
                done_time_counter = 0
            if done_time_counter > AUTO_DOWN:
                return

    @property
    def terminated(self) -> bool:
        raise NotImplementedError

    async def stop_all_coros(self):
        if self._cluster_coroutine and not self._cluster_coroutine.done():
            self._cluster_coroutine.cancel()
            await asyncio.sleep(MINIMUM_SLEEP)
        if self._cluster_stop_handler and not self._cluster_stop_handler.done():
            await self._cluster_stop_handler.cancel()

        for task in self._tasks.values():
            task.stop_all_coroutines()

        await asyncio.sleep(MINIMUM_SLEEP)

    def get_cluster_stopper(self) -> Callable:
        raise NotImplementedError


class NormalClusterManager(ClusterManager):
    def __init__(self, cluster_name: str, cluster_type: JobLaunchType) -> None:
        super().__init__(cluster_name, cluster_type)
        self._backend = sky.backends.CloudVmRayBackend()
        self._cluster_launcher = None
        # self._cluster_event_handler: ClusterEventHandler = None
        self._cluster_coroutine = None

    def create_task(
        self,
        task: sky.Task,
        sky_path_setting: TaskRemotePathSetting,
        event_handler: EventHandler,
        queue_cache: QueryCache,
    ) -> BaseSkyTask:
        return NormalTask(task, sky_path_setting, event_handler, queue_cache)

    async def start_cluster_life_cycle(self, task: sky.Task):
        need_restart = await self.cluster_inspector()
        logger.warning(f"inspect result: Cluster {self._cluster_name} need_restart? {need_restart}.")
        if not need_restart:
            if self._cluster_status == ClusterStatus.UP:
                self._cluster_event_handler.launch_done_event.set()
            return
        # stage 0: waiting cluster to up
        self._cluster_status = ClusterStatus.DUMMY_LAUNCHING
        self._cluster_launcher = ConcurrentProcessHandler(
            functools.partial(sky_error_reraiser, self.dummy_task_launcher(task)),
            event_handler=self._cluster_event_handler,
            name=f"cluster {self._cluster_name}",
        )
        dummy_launched = False
        try:
            dummy_launched, _ = await self._cluster_launcher.status_poller()
        except SkySubProcessError as e:
            raise e
        if not dummy_launched:
            return
        self._cluster_status = ClusterStatus.UP
        # cluster_event_handler: launch_done_event is set
        # stage 1: waiting all tasks to finish
        # await self.wait_all_tasks()

    def dummy_task_launcher(self, task: sky.Task) -> Callable:
        """
        gets the function that launches the cluster and run the setup of the task
        """
        dummy_task = task.to_yaml_config()
        dummy_task["run"] = "echo Dummy"
        task_name = dummy_task.get("name", None)
        dummy_task.update({"name": f"{task_name}-dummy"})
        dummy_task = sky.Task.from_yaml_config(dummy_task)

        return functools.partial(
            sky_dummy_with_auto_stop,
            task=dummy_task,
            cluster_name=self._cluster_name,
            backend=self._backend,
        )

    def get_cluster_stopper(self) -> Callable:
        return functools.partial(sky.down, self._cluster_name)

    def stop_cluster(self):
        """
        stops the cluster if all tasks are done
        """
        if self._cluster_stop_handler is None and self.check_all_done():
            self._cluster_status = ClusterStatus.STOPPING
            self._cluster_stop_handler = BlockingProcessHandler(
                functools.partial(sky_error_reraiser, self.get_cluster_stopper()),
                name=f"cluster {self._cluster_name} stop",
            )
            self._cluster_stop_handler.create_task(timeout=DOWN_TIMEOUT)
            logger.warning(f"Cluster {self._cluster_name} is {self._cluster_status}.")

    @property
    def terminated(self) -> bool:
        """
        1. stopping mode either none or finished
        2. all tasks are done and cluster is done
        """
        return (self._cluster_stop_handler is None or self._cluster_stop_handler.done()) and (
            self.check_all_done() and self._cluster_coroutine is not None and self._cluster_coroutine.done()
        )


class ManagedClusterManager(ClusterManager):
    """
    manager for managed tasks
    """

    def __init__(self, cluster_name: str, cluster_type: JobLaunchType) -> None:
        super().__init__(cluster_name, cluster_type)

    def create_task(
        self,
        task: sky.Task,
        sky_path_setting: TaskRemotePathSetting,
        event_handler: EventHandler,
        queue_cache: QueryCache,
    ) -> BaseSkyTask:
        return ManagedTask(task, sky_path_setting, event_handler, queue_cache)

    async def start_cluster_life_cycle(self, task):
        # stage 0: waiting cluster to up
        # stage 1: waiting all tasks to finish
        self._cluster_event_handler.launch_done_event.set()
        self._cluster_status = ClusterStatus.UP
        # await self.wait_all_tasks()

    def terminated(self) -> bool:
        return False


class ClusterRegistry:
    """
    registry for cluster managers
    """

    def __init__(self) -> None:
        self._clusters: Dict[str, ClusterManager] = {}
        # multiprocessing.set_start_method("fork")

    async def create(self, task: sky.Task, sky_path_setting: TaskRemotePathSetting):
        """
        create (or get) a cluster manager and submit the task
        """
        cluster_name = sky_path_setting.cluster_name
        cluster_type = sky_path_setting.job_type
        self._clusters = {k: v for k, v in self._clusters.items() if not v.terminated or k == cluster_name}
        if cluster_type == JobLaunchType.NORMAL:
            manager = self._clusters.get(cluster_name, NormalClusterManager(cluster_name, cluster_type))
            self._clusters[cluster_name] = manager
        else:
            manager = self._clusters.get(cluster_name, ManagedClusterManager(cluster_name, cluster_type))
            self._clusters[cluster_name] = manager

        await manager.submit(task, sky_path_setting)

    def get(self, resource_meta: SkyPilotMetadata) -> bool:
        """
        returns True if the task is in the cluster
        """
        manager = self._clusters.get(resource_meta.cluster_name, None)
        if not manager:
            return False
        task = manager.tasks.get(resource_meta.job_name, None)
        if not task:
            return False
        return True

    async def clear(self):
        for _cluster in self._clusters.values():
            await _cluster.stop_all_coros()
        self._clusters.clear()

    def empty(self) -> bool:
        return len(self._clusters) == 0

    def stop_cluster(self, sky_path_setting: TaskRemotePathSetting):
        cluster_name = sky_path_setting.cluster_name
        cluster_type = sky_path_setting.job_type
        if cluster_type == JobLaunchType.NORMAL:
            manager = self._clusters.get(cluster_name, NormalClusterManager(cluster_name, cluster_type))
            self._clusters[cluster_name] = manager
        else:
            manager = self._clusters.get(cluster_name, ManagedClusterManager(cluster_name, cluster_type))
            self._clusters[cluster_name] = manager
        self._clusters[cluster_name].stop_cluster()


async def timeout_handler(coroutine: asyncio.coroutines, timeout: int, error_msg: str = None):
    """
    deprecated
    """
    try:
        return await asyncio.wait_for(coroutine, timeout)
    except asyncio.TimeoutError:
        # logger.warning(f"Coroutine {coroutine.__name__} timed out.")
        raise asyncio.TimeoutError(f"Coroutine {coroutine} timed out in {error_msg}.")


def sky_error_reraiser(func: Callable):
    """
    Wrapped function for custom exceptions that require positional arguments
    """
    try:
        return func()
    except Exception:
        error = traceback.format_exc()
        raise SkySubProcessError(error)
