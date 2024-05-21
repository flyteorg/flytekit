from typing import Any, Dict, Optional, Union, Callable, List, Set
from dataclasses import dataclass, asdict
import os
from typing import Any, Callable, Dict, Optional, Union, cast

from google.protobuf.json_format import MessageToDict
import enum
from flytekit import FlyteContextManager, PythonFunctionTask, lazy_module, logger
from flytekit.configuration import DefaultImages, SerializationSettings
from flytekit.core.base_task import PythonTask
from flytekit.core.context_manager import ExecutionParameters
from flytekit.core.python_auto_container import get_registerable_container_image
from flytekit.extend import ExecutionState, TaskPlugins
from flytekit.extend.backend.base_agent import AsyncAgentExecutorMixin
from flytekit.image_spec import ImageSpec
from flytekitplugins.skypilot.metadata import ContainerRunType, JobLaunchType
import sky
from sky import resources as resources_lib
from flytekit.models.literals import LiteralMap

FLYTE_LOCAL_CONFIG = {
    "FLYTE_AWS_ENDPOINT": "http://flyte-sandbox-minio.flyte:9000",
    "FLYTE_AWS_ACCESS_KEY_ID": "minio",
    "FLYTE_AWS_SECRET_ACCESS_KEY": "miniostorage",
}



@dataclass
class SkyPilot(object):
    cluster_name: str
    # accelerators, clouds, regions, spot
    resource_config: Optional[Dict[str, Any]] = None
    file_mounts: Optional[Dict[str, Any]] = None
    local_config: Optional[Dict[str, str]] = None
    setup: Optional[str] = None
    task_name: str = "sky_task"
    prompt_cloud: bool = False
    container_run_type: ContainerRunType = ContainerRunType.RUNTIME
    job_launch_type: JobLaunchType = JobLaunchType.NORMAL
    auto_down: bool = False
    stop_after: int = None
    
    def __post_init__(self):
        # if self.resource_config is None:
        #     self.resource_config = []
        if self.local_config is None:
            self.local_config = {"local_envs": {}}

class SkyPilotFunctionTask(AsyncAgentExecutorMixin, PythonFunctionTask[SkyPilot]):
    
    _TASK_TYPE = "skypilot"
    
    def __init__(
        self,
        task_config: SkyPilot,
        task_function: Callable,
        container_image: Optional[Union[str, ImageSpec]] = None,
        **kwargs,
    ):
        
        # for local testing and remote cloud
        # container_image = replace_local_registry(container_image)
        super(SkyPilotFunctionTask, self).__init__(
            task_config=task_config,
            task_type=self._TASK_TYPE,
            task_function=task_function,
            container_image=container_image,
            **kwargs,
        )

    def get_custom(self, settings: SerializationSettings) -> Dict[str, Any]:
        return asdict(self.task_config)
    
    # deprecated
    def pre_execute(self, user_params: ExecutionParameters | None) -> ExecutionParameters | None:
        import sys
        import pdb
        print("Pre executing...", sys.argv)
        return super().pre_execute(user_params)
    
    def execute(self: PythonTask, **kwargs) -> LiteralMap:
        if isinstance(self.task_config, SkyPilot):
            # Use the Skypilot agent to run it by default.
            try:
                ctx = FlyteContextManager.current_context()
                if not ctx.file_access.is_remote(ctx.file_access.raw_output_prefix):
                    pass
                    # raise ValueError(
                    #     "To submit a Skypilot job locally,"
                    #     " please set --raw-output-data-prefix to a remote path. e.g. s3://, gcs//, etc."
                    # )
                if ctx.execution_state and ctx.execution_state.is_local_execution():
                    return AsyncAgentExecutorMixin.execute(self, **kwargs)
            except Exception as e:
                logger.error(f"Agent failed to run the task with error: {e}")
                logger.info("Falling back to local execution")
        return PythonFunctionTask.execute(self, **kwargs)
    
    def get_image(self, settings: SerializationSettings) -> str:
        if isinstance(self.container_image, ImageSpec):
            # Ensure that the code is always copied into the image, even during fast-registration.
            self.container_image.source_root = settings.source_root

        return get_registerable_container_image(self.container_image, settings.image_config)


    
TaskPlugins.register_pythontask_plugin(SkyPilot, SkyPilotFunctionTask)
