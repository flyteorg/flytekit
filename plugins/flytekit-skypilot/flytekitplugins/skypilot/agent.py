from typing import Optional, List, Dict, Any, Tuple, Callable
from dataclasses import dataclass, asdict
import asyncio
import sky
import sky.check
import sky.cli as sky_cli
import sky.clouds.cloud_registry
import sky.core
import sky.exceptions
from sky.skylet import constants as skylet_constants
import sky.resources
import os
from flytekit.models.literals import LiteralMap
from flytekit import logger, FlyteContextManager
from flytekit.models.task import TaskTemplate
import flytekit
from flytekit import FlyteContext
from flytekit.extend.backend.base_agent import AsyncAgentBase, AgentRegistry, Resource, ResourceMeta
from flytekitplugins.skypilot.utils import skypilot_status_to_flyte_phase, \
    execute_cmd_to_path, RemoteDeletedError, SKY_DIRNAME, SkyPathSetting, TaskRemotePathSetting,\
        setup_cloud_credential, parse_sky_resources, LAUNCH_TYPE_TO_SKY_STATUS
from flytekit.core.data_persistence import FileAccessProvider
from flytekit.core import utils
from flyteidl.core import literals_pb2 as _literals_pb2
from flytekitplugins.skypilot.cloud_registry import CloudRegistry, CloudCredentialError, CloudNotInstalledError
from flytekitplugins.skypilot.metadata import SkyPilotMetadata, ContainerRunType, JobLaunchType
from flytekitplugins.skypilot.workflows import load_sky_config
from google.protobuf.json_format import MessageToDict
from google.protobuf.struct_pb2 import Struct
import textwrap
import multiprocessing
import functools
import traceback
import shlex
from datetime import datetime, timezone


TASK_TYPE = "skypilot"


class WrappedProcess(multiprocessing.Process):
    '''
    Wrapper for multiprocessing.Process to catch exceptions in the target function
    '''
    def __init__(self, *args, **kwargs) -> None:
        multiprocessing.Process.__init__(self, *args, **kwargs)
        self._pconn, self._cconn = multiprocessing.Pipe()
        self._exception = None

    def run(self):
        try:
            multiprocessing.Process.run(self)
            self._cconn.send(None)
        except Exception as e:
            tb = traceback.format_exc()
            self._cconn.send((e, tb))
            #raise e  # You can still rise this exception if you need to

    @property
    def exception(self):
        if self._pconn.poll():
            self._exception = self._pconn.recv()
        return self._exception
    

class BlockingProcessHandler:
    def __init__(self, fn: Callable, check_every_n_sec: int = 5) -> None:
        self._process = WrappedProcess(target=fn)
        self._process.start()
        self._check_interval = check_every_n_sec
        
    async def status_poller(self):
        while self._process.exitcode is None:
            await asyncio.sleep(self._check_interval)
        if self._process.exitcode != 0:
            raise Exception(self._process.exception)
            
    def get_task(self):
        task = asyncio.create_task(self.status_poller())
        return task
    
    def clean_up(self):
        self._process.terminate()
        self._process.join()
        self._process.close()

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
    _delete_coro: asyncio.Task = None
    _status_check_coro: asyncio.Task = None
    _sky_path_setting: TaskRemotePathSetting = None
    _task_name: str = None
    _cluster_name: str = None
    _cancel_callback: Callable = None
    _task_group: asyncio.Future = None
    _launched_process: BlockingProcessHandler = None
    def __init__(self, task_template: TaskTemplate):
        self._task_kwargs = task_template
        args = execute_cmd_to_path(task_template.container.args)
        if self.task_template.custom["job_launch_type"] == JobLaunchType.MANAGED:
            self._cluster_name = sky.jobs.utils.JOB_CONTROLLER_NAME
        else:
            self._cluster_name = self.task_template.custom["cluster_name"]
        
        self._sky_path_setting = TaskRemotePathSetting(
            file_access=FileAccessProvider(
                local_sandbox_dir="/tmp", 
                raw_output_prefix=args["raw_output_data_prefix"]
            ),
            job_type=task_template.custom["job_launch_type"],
            cluster_name=self._cluster_name
        ).from_task_prefix(task_template.custom["task_name"])
        self._task_name = self.sky_path_setting.task_name

    def launch(self):
        setup_cloud_credential()

        cluster_name: str = self.task_template.custom["cluster_name"]
        # sky_resources: List[Dict[str, str]] = task_template.custom["resource_config"]
        sky_resources = parse_sky_resources(self.task_template)
        # build setup commands
        setup_command = SetupCommand(self.task_template)
        # build run commands
        run_command = RunCommand(self.task_template)
        logger.warning(f"Launching task... \nSetup: \n{setup_command.full_setup}\nRun: \n{run_command.full_task_command}")
        sky_resources.update({
            "run": run_command.full_task_command,
            "setup": setup_command.full_setup,
            "name": self._task_name,
        })
        task = sky.Task.from_yaml_config(sky_resources)
        # task.set_resources(sky_resources).set_file_mounts(self.task_template.custom["file_mounts"])
        backend = sky.backends.CloudVmRayBackend()
        job_id = -1
        if self.task_template.custom["job_launch_type"] == JobLaunchType.MANAGED:
            sky.jobs.launch(
                task=task,
                detach_run=True,
                stream_logs=False
            )
        else:
            job_id, _ = sky.launch(
                task=task, 
                cluster_name=cluster_name, 
                backend=backend, 
                idle_minutes_to_autostop=self.task_template.custom["stop_after"],
                down=self.task_template.custom["auto_down"],
                detach_run=True,
                detach_setup=True,
                stream_logs=False
            )
        return job_id

    @property
    def sky_path_setting(self) -> TaskRemotePathSetting:
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

    def launch_failed_callback(
            self, 
            task: asyncio.Task, 
            cause: str, 
            clean_ups: List[Callable]=None,
            cancel_on_done: bool=False
        ):
        if clean_ups is None:
            clean_ups = []
        error_cause = None
        try:
            # check if is cancelled/done/exception
            if task.exception() is not None:
                try:
                    error_cause = "Exception"
                    result = task.result()
                except Exception as e:
                    # re-raise the exception
                    error = traceback.format_exc()
                    self.sky_path_setting.put_error_log(error)
                    logger.error(
                        f"Task {self.task_name} failed to {cause}.\n"
                        f"Cause: \n{error}\n"
                        f"error_log is at {self.sky_path_setting.remote_error_log}"
                    )
                
                self.cancel()
            else:
                # normal exit, if this marks the end of all coroutines, cancel them all
                if cancel_on_done:
                    self.cancel()
        except asyncio.exceptions.CancelledError:
            error_cause = "Cancelled"
        finally:
            for clean_up in clean_ups:
                clean_up()
            logger.warning(f"{cause} coroutine finished with {error_cause}.")

    def launch_process_wrapper(self, fn: Callable):
        self._launched_process = BlockingProcessHandler(fn)
        return self._launched_process.get_task()

    def start(self, callback: Callable=None):
        '''
        create launch coroutine.
        create status check coroutine.
        create remote deleted check coroutine.
        '''
        self._cancel_callback = callback
        self._launch_coro = self.launch_process_wrapper(self.launch)
        self._launch_coro.add_done_callback(
            functools.partial(
                self.launch_failed_callback, 
                cause="launch", 
                clean_ups=[
                    self._launched_process.clean_up,
                    self.sky_path_setting.delete_job
                ]
            )
        )
        self._status_check_coro = asyncio.create_task(self.sky_path_setting.deletion_status())
        self._status_check_coro.add_done_callback(
            functools.partial(
                self.launch_failed_callback, 
                cause="deletion status",
            )
        )
        self._status_upload_coro = asyncio.create_task(self.sky_path_setting.put_task_status())
        self._status_upload_coro.add_done_callback(
            functools.partial(
                self.launch_failed_callback, 
                cause="upload status",
                cancel_on_done=True
            )
        )
        
    def cancel(self):
        if self._cancel_callback is not None:
            self._cancel_callback(self)

class SkyTaskTracker(object):
    _JOB_RESIGTRY: Dict[str, SkyTaskFuture] = {}
    _zip_coro: asyncio.Task = None
    _sky_path_setting: SkyPathSetting = None
    _delete_coro: asyncio.Task = None
    _hostname: str = sky.utils.common_utils.get_user_hash()
    @classmethod
    def try_first_register(cls, task_template: TaskTemplate):
        '''
        sets up coroutine for sky.zip upload
        '''
        if cls._JOB_RESIGTRY:
            return
        args = execute_cmd_to_path(task_template.container.args)
        file_access = FileAccessProvider(
            local_sandbox_dir="/tmp", 
            raw_output_prefix=args["raw_output_data_prefix"]
        )
        
        cls._sky_path_setting = SkyPathSetting(
            task_level_prefix=file_access.raw_output_prefix, 
            unique_id=cls._hostname
        )
        cls._zip_coro = asyncio.create_task(cls._sky_path_setting.zip_and_upload())
        cls._zip_coro.add_done_callback(cls.zip_failed_callback)


    @classmethod
    def zip_failed_callback(self, task: asyncio.Task):
        if task.exception() is not None:
            logger.error(f"Failed to zip and upload the task.\nCause: \n{task.exception()}")
            raise task.exception()

    @classmethod
    def on_task_deleted(cls, deleted_task: SkyTaskFuture):
        '''
        executed on the agent when task on its pod cancelled
        if no tasks on the cluster running, stop the cluster
        the sky.stop part can be disabled if we force autostop
        '''
        import pdb
        # pdb.set_trace()
        if deleted_task._launch_coro and not deleted_task._launch_coro.done():
            deleted_task._launch_coro.cancel()
        if deleted_task._status_check_coro and not deleted_task._status_check_coro.done():
            deleted_task._status_check_coro.cancel()
        if deleted_task._status_upload_coro and not deleted_task._status_upload_coro.done():
            deleted_task._status_upload_coro.cancel()
        
        running_tasks_on_cluster = list(filter(
            lambda task: task.task_name != deleted_task.task_name and task.cluster_name == deleted_task.cluster_name, 
            cls._JOB_RESIGTRY.values()
        ))
        if not running_tasks_on_cluster:
            logger.warning(f"Stopping cluster {deleted_task.cluster_name}")
            try:
                # FIXME: this is a blocking call, delete needs long timeout
                sky.stop(deleted_task.cluster_name)
            except sky.exceptions.NotSupportedError as e:
                logger.warning(f"Cluster {deleted_task.cluster_name} is not supported for stopping.")
        
        cls._sky_path_setting.remote_path_setting.delete_task(deleted_task.sky_path_setting.unique_id)
        # del cls._JOB_RESIGTRY[deleted_task.task_name]
    
    @classmethod
    def register_sky_task(cls, task_template: TaskTemplate):
        cls.try_first_register(task_template)
        new_task = SkyTaskFuture(task_template)
        started = cls._sky_path_setting.remote_path_setting.task_exists(new_task.sky_path_setting.unique_id)
        if started:
            logger.warning(f"{new_task.task_name} task has already been created.")
            return new_task

        cls._sky_path_setting.remote_path_setting.touch_task(new_task.sky_path_setting.unique_id)
        new_task.start(callback=cls.on_task_deleted)
        cls._JOB_RESIGTRY[new_task.task_name] = new_task
        return new_task

def remote_setup(remote_meta: SkyPilotMetadata, wrapped, **kwargs):
    sky_path_setting = SkyPathSetting(
        task_level_prefix=remote_meta.task_metadata_prefix, 
        unique_id=remote_meta.tracker_hostname
    )
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
    

def query_job_status(resource_meta: SkyPilotMetadata):
    # task on another agent pod may fail to launch, check for launch error log
    sky_path_setting = TaskRemotePathSetting(
        file_access=FileAccessProvider(
            local_sandbox_dir="/tmp", 
            raw_output_prefix=resource_meta.task_metadata_prefix
        ),
        job_type=resource_meta.job_launch_type,
        cluster_name=resource_meta.cluster_name,
        task_name=resource_meta.job_name
    )

    # check job status
    return LAUNCH_TYPE_TO_SKY_STATUS[resource_meta.job_launch_type](sky_path_setting.get_task_status().task_status)
        
    # return sky.JobStatus.INIT  # job not found, in most cases this is in setup stage

def check_remote_agent_alive(resource_meta: SkyPilotMetadata):
    sky_path_setting = SkyPathSetting(
        task_level_prefix=resource_meta.task_metadata_prefix, 
        unique_id=resource_meta.tracker_hostname
    )
    utc_time = datetime.now(timezone.utc)
    last_upload_time = sky_path_setting.last_upload_time()
    if last_upload_time is not None:
        time_diff = utc_time - last_upload_time
        if time_diff.total_seconds() > skylet_constants.CONTROLLER_IDLE_MINUTES_TO_AUTOSTOP * 60:
            return False
        
    return True
    

def remote_deletion(resource_meta: SkyPilotMetadata):
    # this part can be removed if sky job controller down is supported
    # if the zip is not updated for a long time, the agent pod is considered down, so we need to delete the controller
    if not check_remote_agent_alive(resource_meta):
        with multiprocessing.Pool(1) as p:
            starmap_results = p.starmap(
                functools.partial(remote_setup, cluster_name=resource_meta.cluster_name), 
                [(resource_meta, sky.down)]
            )
    sky_task_settings = TaskRemotePathSetting(
        file_access=FileAccessProvider(
            local_sandbox_dir="/tmp", 
            raw_output_prefix=resource_meta.task_metadata_prefix
        ),
        job_type=resource_meta.job_launch_type,
        cluster_name=resource_meta.cluster_name,
        task_name=resource_meta.job_name
    )
    sky_task_settings.to_proto_and_upload(resource_meta, sky_task_settings.remote_delete_proto)


    

class SkyPilotAgent(AsyncAgentBase):
    def __init__(self):
        super().__init__(task_type_name=TASK_TYPE, metadata_type=SkyPilotMetadata)
        # TODO: download sky config

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
        meta = SkyPilotMetadata(
            job_name=task.task_name,
            cluster_name=task.cluster_name,
            task_metadata_prefix=task.sky_path_setting.file_access.raw_output_prefix,
            tracker_hostname=SkyTaskTracker._hostname,
            job_launch_type=task_template.custom["job_launch_type"],
        )
        
        return meta
        


    async def get(self, resource_meta: SkyPilotMetadata, **kwargs) -> Resource:
        import pdb
        # pdb.set_trace()
        received_time = datetime.now(timezone.utc)
        job_status = None
        outputs = None
        job_status = query_job_status(resource_meta)
        
        logger.warning(f"Getting... {job_status}, took {(datetime.now(timezone.utc) - received_time).total_seconds()}")
        phase = skypilot_status_to_flyte_phase(job_status)
        return Resource(phase=phase, outputs=outputs, message=None)

    async def delete(self, resource_meta: SkyPilotMetadata, **kwargs):
        import pdb
        # pdb.set_trace()
        if resource_meta.job_name not in SkyTaskTracker._JOB_RESIGTRY:
            remote_deletion(resource_meta)
            
        else:
            existed_task = SkyTaskTracker._JOB_RESIGTRY[resource_meta.job_name]
            existed_task.cancel()
            
# To register the skypilot agent
AgentRegistry.register(SkyPilotAgent())
