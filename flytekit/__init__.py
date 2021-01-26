import flytekit.plugins  # noqa: F401
from flytekit.annotated.base_sql_task import SQLTask
from flytekit.annotated.base_task import TaskMetadata, kwtypes
from flytekit.annotated.condition import conditional
from flytekit.annotated.container_task import ContainerTask
from flytekit.annotated.context_manager import FlyteContext
from flytekit.annotated.dynamic_workflow_task import dynamic
from flytekit.annotated.launch_plan import LaunchPlan
from flytekit.annotated.map_task import maptask
from flytekit.annotated.reference import get_reference_entity
from flytekit.annotated.reference_entity import TaskReference, WorkflowReference
from flytekit.annotated.resources import Resources
from flytekit.annotated.task import reference_task, task
from flytekit.annotated.workflow import WorkflowFailurePolicy, reference_workflow, workflow
from flytekit.loggers import logger

__version__ = "0.16.0b0"


def current_context():
    """
    Use this method to get a handle of specific parameters available in a flyte task.

    Usage

    .. code-block::

        flytekit.current_context().logging.info(...)

    Available params are documented in :py:class:`flytekit.annotated.context_manager.ExecutionParams`.
    There are some special params, that should be available
    """
    return FlyteContext.current_context().user_space_params
