import asyncio
import functools
import multiprocessing
import os
from datetime import datetime, timezone
from typing import Dict, Optional

import sky
import sky.core
import sky.exceptions
import sky.resources
from flytekitplugins.skypilot.metadata import JobLaunchType, SkyPilotMetadata
from flytekitplugins.skypilot.task_utils import get_sky_task_config
from flytekitplugins.skypilot.utils import (
    LAUNCH_TYPE_TO_SKY_STATUS,
    ClusterRegistry,
    SkyPathSetting,
    TaskCreationIdentifier,
    TaskRemotePathSetting,
    skypilot_status_to_flyte_phase,
)
from sky.skylet import constants as skylet_constants

from flytekit import logger
from flytekit.core.data_persistence import FileAccessProvider
from flytekit.extend.backend.base_agent import AgentRegistry, AsyncAgentBase, Resource
from flytekit.models.literals import LiteralMap
from flytekit.models.task import TaskExecutionMetadata, TaskTemplate

TASK_TYPE = "skypilot"


class SkyTaskFuture(object):
    _task_kwargs: TaskTemplate = None
    _sky_path_setting: TaskRemotePathSetting = None
    _task_name: str = None
    _cluster_name: str = None

    def __init__(self, task_template: TaskTemplate, task_identifier: TaskCreationIdentifier):
        self._task_kwargs = task_template
        if self.task_template.custom["job_launch_type"] == JobLaunchType.MANAGED:
            self._cluster_name = sky.jobs.utils.JOB_CONTROLLER_NAME
        else:
            self._cluster_name = self.task_template.custom["cluster_name"]

        self._sky_path_setting = TaskRemotePathSetting(
            file_access=FileAccessProvider(local_sandbox_dir="/tmp", raw_output_prefix=task_identifier.output_prefix),
            job_type=task_template.custom["job_launch_type"],
            cluster_name=self._cluster_name,
            unique_id=task_identifier.task_id,
        ).from_task_prefix(task_template.custom["task_name"])
        self._task_name = self.sky_path_setting.task_name

    def get_task(self):
        cluster_name: str = self.task_template.custom["cluster_name"]
        sky_resources = get_sky_task_config(self.task_template)
        sky_resources.update(
            {
                "name": self.task_name,
            }
        )
        task = sky.Task.from_yaml_config(sky_resources)
        return task, cluster_name

    @property
    def sky_path_setting(self) -> TaskRemotePathSetting:
        return self._sky_path_setting

    @property
    def task_template(self) -> TaskTemplate:
        return self._task_kwargs

    @property
    def task_name(self) -> str:
        return self._task_name

    @property
    def cluster_name(self) -> str:
        return self._cluster_name


class SkyTaskTracker(object):
    _JOB_RESIGTRY: Dict[str, SkyTaskFuture] = {}
    _zip_coro: asyncio.Task = None
    _sky_path_setting: SkyPathSetting = None
    _hostname: str = sky.utils.common_utils.get_user_hash()
    _CLUSTER_REGISTRY: ClusterRegistry = ClusterRegistry()

    @classmethod
    def try_first_register(cls, task_template: TaskTemplate, task_identifier: TaskCreationIdentifier):
        """
        sets up coroutine for sky.zip upload
        """
        if not cls._CLUSTER_REGISTRY.empty():
            return
        task_identifier._output_prefix = task_identifier.output_prefix.replace("s3://my-s3-bucket", "s3://dgx-agent/test")
        file_access = FileAccessProvider(local_sandbox_dir="/tmp", raw_output_prefix=task_identifier.output_prefix)

        # print(file_access.raw_output_prefix)
        file_access._raw_output_prefix = file_access.raw_output_prefix.replace("s3://my-s3-bucket", "s3://dgx-agent/test")
        # print(file_access.raw_output_prefix)
        cls._sky_path_setting = SkyPathSetting(task_level_prefix=file_access.raw_output_prefix, unique_id=cls._hostname)
        cls._zip_coro = asyncio.create_task(cls._sky_path_setting.zip_and_upload())
        cls._zip_coro.add_done_callback(cls.zip_failed_callback)

    @classmethod
    def zip_failed_callback(self, task: asyncio.Task):
        try:
            if task.exception() is not None:
                logger.error(f"Failed to zip and upload the task.\nCause: \n{task.exception()}")
                raise task.exception()
        except asyncio.exceptions.CancelledError:
            pass

    @classmethod
    def register_sky_task(cls, task_template: TaskTemplate, task_identifier: TaskCreationIdentifier):
        cls.try_first_register(task_template, task_identifier)
        new_task = SkyTaskFuture(task_template, task_identifier)
        started = cls._sky_path_setting.remote_path_setting.task_exists(new_task.sky_path_setting.unique_id)
        if started:
            logger.warning(f"{new_task.task_name} task has already been created.")
            return new_task

        print("_sky_path_setting.remote_path_setting")
        cls._sky_path_setting.remote_path_setting.touch_task(new_task.sky_path_setting.unique_id)
        print("new_task.get_task()")
        task, _ = new_task.get_task()
        cls._CLUSTER_REGISTRY.create(task, new_task.sky_path_setting)
        return new_task


def remote_setup(remote_meta: SkyPilotMetadata, wrapped, **kwargs):
    sky_path_setting = SkyPathSetting(
        task_level_prefix=remote_meta.task_metadata_prefix, unique_id=remote_meta.tracker_hostname
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
        os.path.join(home_sky_dir, "state.db"), sky.global_user_state.create_table
    )
    sky.skylet.job_lib._DB = sky.utils.db_utils.SQLiteConn(
        os.path.join(home_sky_dir, "skylet.db"), sky.skylet.job_lib.create_table
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
        file_access=FileAccessProvider(local_sandbox_dir="/tmp", raw_output_prefix=resource_meta.task_metadata_prefix),
        job_type=resource_meta.job_launch_type,
        cluster_name=resource_meta.cluster_name,
        task_name=resource_meta.job_name,
    )

    task_status = sky_path_setting.get_task_status().task_status
    return LAUNCH_TYPE_TO_SKY_STATUS[resource_meta.job_launch_type](task_status)


def check_remote_agent_alive(resource_meta: SkyPilotMetadata):
    sky_path_setting = SkyPathSetting(
        task_level_prefix=resource_meta.task_metadata_prefix, unique_id=resource_meta.tracker_hostname
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
            p.starmap(
                functools.partial(remote_setup, cluster_name=resource_meta.cluster_name), [(resource_meta, sky.down)]
            )
    sky_task_settings = TaskRemotePathSetting(
        file_access=FileAccessProvider(local_sandbox_dir="/tmp", raw_output_prefix=resource_meta.task_metadata_prefix),
        job_type=resource_meta.job_launch_type,
        cluster_name=resource_meta.cluster_name,
        task_name=resource_meta.job_name,
    )
    sky_task_settings.to_proto_and_upload(resource_meta, sky_task_settings.remote_delete_proto)


class SkyPilotAgent(AsyncAgentBase):
    def __init__(self):
        super().__init__(task_type_name=TASK_TYPE, metadata_type=SkyPilotMetadata)

    async def create(
        self,
        task_template: TaskTemplate,
        inputs: Optional[LiteralMap] = None,
        output_prefix: Optional[str] = None,
        task_execution_metadata: Optional[TaskExecutionMetadata] = None,
        **kwargs,
    ) -> SkyPilotMetadata:
        print("create")
        task_identifier = TaskCreationIdentifier(output_prefix, task_execution_metadata)
        task = SkyTaskTracker.register_sky_task(task_template=task_template, task_identifier=task_identifier)
        logger.warning(f"Created SkyPilot {task.task_name}")
        meta = SkyPilotMetadata(
            job_name=task.task_name,
            cluster_name=task.cluster_name,
            task_metadata_prefix=task.sky_path_setting.file_access.raw_output_prefix,
            tracker_hostname=SkyTaskTracker._hostname,
            job_launch_type=task_template.custom["job_launch_type"],
        )
        # pdb.set_trace()
        return meta

    async def get(self, resource_meta: SkyPilotMetadata, **kwargs) -> Resource:
        # pdb.set_trace()
        received_time = datetime.now(timezone.utc)
        job_status = None
        outputs = None
        job_status = query_job_status(resource_meta)

        logger.warning(f"Getting... {job_status}, took {(datetime.now(timezone.utc) - received_time).total_seconds()}")
        phase = skypilot_status_to_flyte_phase(job_status)
        return Resource(phase=phase, outputs=outputs, message=None)

    async def delete(self, resource_meta: SkyPilotMetadata, **kwargs):
        print("delete")
        remote_deletion(resource_meta)


# To register the skypilot agent
AgentRegistry.register(SkyPilotAgent())
