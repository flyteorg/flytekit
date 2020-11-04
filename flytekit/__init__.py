import flytekit.plugins  # noqa: F401
from flytekit.loggers import logger
from flytekit.annotated.context_manager import FlyteContext
from flytekit.annotated.task import task, dynamic, ContainerTask, SQLTask, maptask, metadata
from flytekit.annotated.workflow import workflow
from flytekit.annotated.launch_plan import LaunchPlan

__version__ = "1.0.0a0"


def current_context():
    return FlyteContext.current_context().user_space_params
