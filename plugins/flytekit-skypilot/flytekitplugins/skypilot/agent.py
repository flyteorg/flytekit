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
    execute_cmd_to_path, RemoteDeletedError, SKY_DIRNAME, SkyPathSetting, TaskRemotePathSetting
from flytekitplugins.skypilot.task import ContainerRunType, JobLaunchType
from flytekit.core.data_persistence import FileAccessProvider
from flytekit.core import utils
from flyteidl.core import literals_pb2 as _literals_pb2
from flytekitplugins.skypilot.cloud_registry import CloudRegistry, CloudCredentialError, CloudNotInstalledError
from flytekitplugins.skypilot.metadata import SkyPilotMetadata
from google.protobuf.json_format import MessageToDict
from google.protobuf.struct_pb2 import Struct
import textwrap
import multiprocessing
import functools
import tempfile
import shlex
from datetime import datetime, timezone


TASK_TYPE = "skypilot"




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
    _cloud_checked: bool = False
    _cancel_callback: Callable = None
    def __init__(self, task_template: TaskTemplate):
        self._task_kwargs = task_template
        args = execute_cmd_to_path(task_template.container.args)
        self._sky_path_setting = TaskRemotePathSetting(
            file_access=FileAccessProvider(
                local_sandbox_dir="/tmp", 
                raw_output_prefix=args["raw_output_data_prefix"]
            ),
        )
        task_suffix = self._sky_path_setting.unique_id
        self._task_name = f'{self.task_template.custom["task_name"]}.{task_suffix}'
        if self.task_template.custom["job_launch_type"] == JobLaunchType.MANAGED:
            self._cluster_name = sky.jobs.utils.JOB_CONTROLLER_NAME
        else:
            self._cluster_name = self.task_template.custom["cluster_name"]
        


    def launch(self):
        if not SkyTaskTracker._cloud_checked and not self.cloud_checked:
            setup_cloud_credential()
            load_sky_config()
            self._cloud_checked = True

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
                # cluster=cluster_name,
                # backend=backend,
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

    @property
    def cloud_checked(self) -> bool:
        return self._cloud_checked

    def launch_failed_callback(self, task: asyncio.Task):
        if task.exception() is not None:
            self.sky_path_setting.put_error_log(task.exception())
            logger.error(
                f"Task {self.task_name} failed to launch.\n"
                f"Cause: \n{task.exception()}\n"
                f"error_log is at {self.sky_path_setting.remote_error_log}"
            )
            
    def task_deleted_callback(self, task: asyncio.Task):
        # when remotely triggered it enters the try block. If deleted-on pod, 
        # it enters the except block when returning from agent `delete` function
        import pdb
        # pdb.set_trace()
        try:
            if type(task.exception()) == RemoteDeletedError:
                self.sky_path_setting.put_error_log(task.exception())
                logger.error(
                    f"Task {self.task_name} failed to zip and upload.\n"
                    f"Cause: \n{task.exception()}\n"
                    f"error_log is at {self.sky_path_setting.remote_error_log}"
                )
                self.cancel()
        except asyncio.exceptions.CancelledError:
            pass

    def start(self, callback: Callable=None):
        '''
        # create launch coroutine. If put error log if launch failed
        # create status check coroutine. If deletion posted by other pods, self cancel task
        '''
        self._cancel_callback = callback
        self._launch_coro = asyncio.create_task(asyncio.to_thread(self.launch))
        # self._launch_coro.add_done_callback(helper_done_callback)
        self._launch_coro.add_done_callback(self.launch_failed_callback)
        self._status_check_coro = asyncio.create_task(self.sky_path_setting.deletion_status())
        self._status_check_coro.add_done_callback(self.task_deleted_callback)
    
    def cancel(self):
        if self._launch_coro is not None and not self._launch_coro.done():
            self._launch_coro.cancel()
        if self._status_check_coro is not None:
            self._status_check_coro.cancel()
        
        if self._cancel_callback is not None:
            self._cancel_callback(self)

class SkyTaskTracker(object):
    _JOB_RESIGTRY: Dict[str, SkyTaskFuture] = {}
    _cloud_checked: bool = False
    _zip_coro: asyncio.Task = None
    _sky_path_setting: SkyPathSetting = None
    _delete_coro: asyncio.Task = None
    _hostname: str = sky.utils.common_utils.get_user_hash()
    @classmethod
    def try_first_register(cls, task_template: TaskTemplate):
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


    @classmethod
    def on_task_deleted(cls, deleted_task: SkyTaskFuture):
        '''
        executed on the agent when task on its pod cancelled
        if no tasks on the cluster running, stop the cluster
        the sky.stop part can be disabled if we force autostop
        '''
        import pdb
        # pdb.set_trace()
        running_tasks_on_cluster = list(filter(
            lambda task: task.task_name != deleted_task.task_name and task.cluster_name == deleted_task.cluster_name, 
            cls._JOB_RESIGTRY.values()
        ))
        if not running_tasks_on_cluster:
            logger.warning(f"Stopping cluster {deleted_task.cluster_name}")
            deleted_task._delete_coro = asyncio.create_task(
                asyncio.to_thread(sky.stop, deleted_task.cluster_name)
            )
            # sky.stop(deleted_task.cluster_name)
        
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
        cls._cloud_checked = cls._cloud_checked or \
            any([task.cloud_checked for task in SkyTaskTracker._JOB_RESIGTRY.values()])

        cls._JOB_RESIGTRY[new_task.task_name] = new_task
        return new_task


def get_job_list(resource_meta: SkyPilotMetadata) -> List[Dict[str, Any]]:
    if resource_meta.job_launch_type == JobLaunchType.NORMAL:
        return sky.queue(resource_meta.cluster_name)
    return sky.jobs.queue(refresh=True)


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
        )
    )
    if sky_path_setting.remote_failed():
        local_log_file = sky_path_setting.file_access.get_random_local_path()
        sky_path_setting.file_access.get_data(sky_path_setting.remote_error_log, local_log_file)
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
    job_list = get_job_list(resource_meta)
    for job in job_list:
        if job["job_name"] == resource_meta.job_name:
            return job["status"]
        
    return sky.JobStatus.INIT  # job not found, in most cases this is in setup stage

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
        )
    )
    meta_proto = Struct()
    meta_proto.update(resource_meta.__dict__)
    # save to meta_proto to proto file
    with tempfile.NamedTemporaryFile(mode="wb") as f:
        f.write(meta_proto.SerializeToString())
        f.flush()
        sky_task_settings.remote_delete(f.name)

    

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
        
        logger.warning(f"Getting... {job_status}, took {(datetime.now(timezone.utc) - received_time).total_seconds()}")
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
def load_sky_config():
    secret_manager = flytekit.current_context().secrets
    try:
        config_url = secret_manager.get(
            group="sky",
            key="config",
        )
    except ValueError as e:
        logger.warning(f"sky config not set, will use default controller setting")
        return

    ctx = FlyteContextManager.current_context()
    file_access = ctx.file_access
    file_access.get_data(config_url, sky.skypilot_config.CONFIG_PATH)
    sky.skypilot_config._try_load_config()

AgentRegistry.register(SkyPilotAgent())
