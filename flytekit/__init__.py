import flytekit.plugins
from flytekit.core.base_sql_task import SQLTask
from flytekit.core.base_task import TaskMetadata, kwtypes
from flytekit.core.condition import conditional
from flytekit.core.container_task import ContainerTask
from flytekit.core.context_manager import ExecutionParameters, FlyteContext
from flytekit.core.dynamic_workflow_task import dynamic
from flytekit.core.launch_plan import LaunchPlan
from flytekit.core.map_task import maptask
from flytekit.core.notification import Email, PagerDuty, Slack
from flytekit.core.python_function_task import PythonFunctionTask
from flytekit.core.reference import get_reference_entity
from flytekit.core.reference_entity import LaunchPlanReference, TaskReference, WorkflowReference
from flytekit.core.resources import Resources
from flytekit.core.schedule import CronSchedule, FixedRate
from flytekit.core.task import reference_task, task
from flytekit.core.workflow import WorkflowFailurePolicy, reference_workflow, workflow
from flytekit.loggers import logger

__version__ = "0.16.0b6"


def current_context() -> ExecutionParameters:
    """
    Use this method to get a handle of specific parameters available in a flyte task.

    Usage

    .. code-block::

        flytekit.current_context().logging.info(...)

    Available params are documented in :py:class:`flytekit.core.context_manager.ExecutionParams`.
    There are some special params, that should be available
    """
    return FlyteContext.current_context().user_space_params
