import flytekit.plugins  # noqa: F401
from flytekit.annotated.context_manager import FlyteContext
from flytekit.annotated.launch_plan import LaunchPlan
from flytekit.annotated.task import ContainerTask, Reference, SQLTask, dynamic, kwtypes, maptask, metadata, task
from flytekit.annotated.workflow import workflow
from flytekit.loggers import logger

__version__ = "0.16.0a0"


def current_context():
    return FlyteContext.current_context().user_space_params
