import flytekit.plugins  # noqa: F401
from flytekit.core.base_sql_task import SQLTask
from flytekit.core.base_task import TaskMetadata, kwtypes
from flytekit.core.container_task import ContainerTask
from flytekit.core.context_manager import FlyteContext
from flytekit.core.dynamic_workflow_task import dynamic
from flytekit.core.launch_plan import LaunchPlan
from flytekit.core.map_task import maptask
from flytekit.core.reference import get_reference_entity
from flytekit.core.reference_entity import TaskReference, WorkflowReference
from flytekit.core.resources import Resources
from flytekit.core.task import reference_task, task
from flytekit.core.workflow import WorkflowFailurePolicy, reference_workflow, workflow
from flytekit.loggers import logger

__version__ = "0.16.0a2"


def current_context():
    """
    Use this method to get a handle of specific parameters available in a flyte task.

    Usage

    .. code-block::

        flytekit.current_context().logging.info(...)

    Available params are documented in :py:class:`flytekit.core.context_manager.ExecutionParams`.
    There are some special params, that should be available
    """
    return FlyteContext.current_context().user_space_params
