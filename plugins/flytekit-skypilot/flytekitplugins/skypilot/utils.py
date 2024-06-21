import asyncio
import enum
import functools
import multiprocessing
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
    SKY_DIRNAME,
)
from flytekitplugins.skypilot.metadata import JobLaunchType, SkyPilotMetadata
from flytekitplugins.skypilot.process_utils import (
    BaseProcessHandler,
    BlockingProcessHandler,
    ClusterEventHandler,
    ConcurrentProcessHandler,
    EventHandler,
)
from google.protobuf.json_format import MessageToDict
from google.protobuf.struct_pb2 import Struct

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
    DOWN = enum.auto()
    INIT = enum.auto()
    DUMMY_LAUNCHING = enum.auto()
    UP = enum.auto()
    FAILED = enum.auto()
    WAIT_FOR_TASKS = enum.auto()
    STOPPING = enum.auto()


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
    # reinitialize the db connection
    logger.warning("Reinitializing the db connection")
    db_path = sky.global_user_state._DB.db_path
    sky.global_user_state._DB.conn.close()
    sky.global_user_state._DB = sky.utils.db_utils.SQLiteConn(db_path, sky.global_user_state.create_table)


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
        self._cancel_task_coroutine: Optional[asyncio.Task] = None

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

    def cancel_task_callback(self, task: asyncio.Task):
        self._task_event_handler.finished_event.set()
        if task.exception() is not None:
            raise task.exception()

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
                    error = traceback.format_exc()
                    self._sky_path_setting.put_error_log(e)
                    logger.error(
                        f"Task {self._sky_path_setting.task_name} failed to {cause}.\n"
                        f"Cause: \n{error}\n"
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
            # pdb.set_trace()
            if self._task_status == TaskStatus.TASK_SUBMITTED and self._task_event_handler.is_terminal():
                self._task_status = TaskStatus.DONE
                if not self._task_event_handler.finished_event.is_set():
                    cancel_process_handler = BlockingProcessHandler(
                        functools.partial(sky_error_reraiser, self.cancel_task),
                        name=f"cancel {self._sky_path_setting.task_name}",
                    )
                    cancel_process_handler.create_task()
                    self._cancel_task_coroutine = cancel_process_handler._task
                    self._cancel_task_coroutine.add_done_callback(self.cancel_task_callback)
            # breakpoint()
            if self._cancel_task_coroutine is None:
                self._task_event_handler.finished_event.set()

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
        # stage 0: waiting cluster to up
        while not self._cluster_event_handler.launch_done_event.is_set():
            # cluster up failed
            if self._cluster_event_handler.failed_event.is_set():
                self._task_event_handler.failed_event.set()
                return False
            # cancelled during cluster up
            deleted = await self._sky_path_setting.delete_done(self._task_event_handler)
            if deleted:
                return False
            await asyncio.sleep(COROUTINE_INTERVAL)

        return True

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
        launched, launch_result = await self._launch_fn_handler.status_poller(
            extra_events=[self._cluster_event_handler.is_terminal]
        )
        if not launched:
            return
        job_id, _ = launch_result
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
            self._task_status = TaskStatus.DONE

    def put_error_log(self, error_log: Exception):
        self._sky_path_setting.put_error_log(error_log)


class NormalTask(BaseSkyTask):
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

    def get_queue_handler(self) -> Callable:
        return functools.partial(sky.queue, cluster_name=self.cluster_name)

    def get_launch_handler(self, task: sky.Task) -> Callable:
        return functools.partial(
            sky.exec,
            task=task,
            cluster_name=self.cluster_name,
            stream_logs=False,
            backend=self._backend,
            detach_run=False,
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
        return functools.partial(sky.jobs.launch, task=task, detach_run=True, stream_logs=False)

    def cancel_task(self):
        sky.jobs.cancel(name=self._sky_path_setting.task_name)

    def launch(self, task: sky.Task):
        sky.jobs.launch(task=task, detach_run=True, stream_logs=False)


class ClusterManager(object):
    def __init__(self, cluster_name: str, cluster_type: JobLaunchType) -> None:
        self._cluster_name: str = cluster_name
        self._cluster_type: JobLaunchType = cluster_type
        self._query_cache: QueryCache = QueryCache()
        self._tasks: Dict[str, BaseSkyTask] = {}
        self._cluster_status = ClusterStatus.DOWN
        self._cluster_stop_handler: Optional[BaseProcessHandler] = None
        self._cluster_launcher: Optional[BaseProcessHandler] = None
        self._cluster_coroutine: Optional[asyncio.Task] = None

    def launch(self, task: sky.Task, last_status: List[Dict[str, Any]] = None):
        raise NotImplementedError

    @property
    def tasks(self):
        return self._tasks

    def reset(self):
        self._tasks.clear()
        self._cluster_status = ClusterStatus.INIT
        self._cluster_launcher = None
        self._cluster_coroutine = None
        if self._cluster_stop_handler and not self._cluster_stop_handler.done():
            self._cluster_stop_handler.cancel()
            logger.warning(f"Cluster {self._cluster_name} stop coroutine cancelled.")
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

    def submit(self, task: sky.Task, sky_path_setting: TaskRemotePathSetting):
        start_cluster = False
        if (
            self._cluster_coroutine is None or self._cluster_coroutine.done()
        ) and self._cluster_status != ClusterStatus.INIT:
            self.reset()
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

    def cluster_up_callback(self, task: asyncio.Task, clean_ups: List[Callable] = None):
        cause = "cluster up"
        if clean_ups is None:
            clean_ups = []
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
                    error = traceback.format_exc()
                    logger.error(f"Cluster {self._cluster_name} failed to {cause}.\n" f"Cause: \n{error}\n")
                    put_error = e
                finally:
                    # failed to launch cluster
                    if self._cluster_status < ClusterStatus.UP:
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
            if not (
                task._task_event_handler.finished_event.is_set()
                or (task._task_event_handler.is_terminal() and task._task_status < TaskStatus.TASK_SUBMITTED)
            ):
                return False
        return True

    def stop_cluster(self):
        # TODO: change to force autostop/autodown
        return

    async def wait_all_tasks(self):
        """
        periodically check if all tasks are done
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


class NormalClusterManager(ClusterManager):
    def __init__(self, cluster_name: str, cluster_type: JobLaunchType) -> None:
        super().__init__(cluster_name, cluster_type)
        self._backend = sky.backends.CloudVmRayBackend()
        self._cluster_launcher = None
        self._cluster_event_handler: ClusterEventHandler = None
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
        # stage 0: waiting cluster to up
        self._cluster_status = ClusterStatus.DUMMY_LAUNCHING
        self._cluster_launcher = BlockingProcessHandler(
            functools.partial(sky_error_reraiser, functools.partial(self.launch_dummy_task, task=task)),
            event_handler=self._cluster_event_handler,
            name=f"cluster {self._cluster_name}",
        )
        dummy_launched, _ = await self._cluster_launcher.status_poller()
        if not dummy_launched:
            return
        self._cluster_status = ClusterStatus.UP
        # cluster_event_handler: launch_done_event is set
        # stage 1: waiting all tasks to finish
        await self.wait_all_tasks()

    def launch_dummy_task(self, task: sky.Task):
        """
        launches the cluster and run the setup of the task
        """
        dummy_task = task.to_yaml_config()
        dummy_task["run"] = "echo Dummy"
        task_name = dummy_task.get("name", None)
        dummy_task.update({"name": f"{task_name}-dummy"})
        dummy_task = sky.Task.from_yaml_config(dummy_task)
        try:
            sky.autostop(self._cluster_name, -1)
        except (ValueError, sky.exceptions.ClusterNotUpError):
            pass
        sky.launch(
            task=dummy_task,
            cluster_name=self._cluster_name,
            backend=self._backend,
            stream_logs=False,
            detach_setup=True,
            detach_run=False,
            idle_minutes_to_autostop=AUTO_DOWN / 60,
        )

    def stop_cluster(self):
        """
        stops the cluster if all tasks are done
        """
        if self._cluster_stop_handler is None and self.check_all_done():
            self._cluster_status = ClusterStatus.STOPPING
            self._cluster_stop_handler = BlockingProcessHandler(
                functools.partial(sky_error_reraiser, functools.partial(sky.stop, self._cluster_name)),
                name=f"cluster {self._cluster_name} stop",
            )
            self._cluster_stop_handler.create_task(timeout=DOWN_TIMEOUT)
            logger.warning(f"Cluster {self._cluster_name} is {self._cluster_status}.")


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
        await self.wait_all_tasks()


class ClusterRegistry:
    """
    registry for cluster managers
    """

    def __init__(self) -> None:
        self._clusters: Dict[str, ClusterManager] = {}
        multiprocessing.set_start_method("fork")

    def create(self, task: sky.Task, sky_path_setting: TaskRemotePathSetting):
        """
        create (or get) a cluster manager and submit the task
        """
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

    def clear(self):
        self._clusters.clear()

    def empty(self) -> bool:
        return len(self._clusters) == 0


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
    except Exception as e:
        raise Exception(str(e))
