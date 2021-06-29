"""Module defining main Flyte backend entrypoint."""

from functools import singledispatchmethod

from flytekit.core.launch_plan import LaunchPlan
from flytekit.core.python_function_task import PythonFunctionTask
from flytekit.core.workflow import PythonFunctionWorkflow
from flytekit.remote.launch_plan import FlyteLaunchPlan
from flytekit.remote.tasks.task import FlyteTask
from flytekit.remote.workflow import FlyteWorkflow
from flytekit.remote.workflow_execution import FlyteWorkflowExecution


class FlyteRemote(object):
    """Entrypoint class for accessing Flyte remote backend."""

    def __init__(self):
        pass

    def fetch_task(self, project, domain, name, version=None) -> FlyteTask:
        pass

    def fetch_workflow(self, project, domain, name, version=None) -> FlyteWorkflow:
        pass

    def fetch_launchplan(self, project, domain, name, version=None) -> FlyteLaunchPlan:
        pass

    def fetch_workflow_execution(self, project, domain, name) -> FlyteWorkflowExecution:
        pass

    #####################
    # Register Entities #
    #####################

    @singledispatchmethod
    def register(self, entity):
        raise NotImplementedError(f"entity type {type(entity)} not recognized for registration")

    # Flyte Remote Entities
    # ---------------------

    @register.register
    def _(self, entity: FlyteTask):
        pass

    @register.register
    def _(self, entity: FlyteWorkflow):
        pass

    @register.register
    def _(self, entity: FlyteWorkflowExecution):
        pass

    @register.register
    def _(self, entity: FlyteLaunchPlan):
        pass

    # Flytekit Entities
    # -----------------

    @register.register
    def _(self, entity: PythonFunctionTask):
        pass

    @register.register
    def _(self, entity: PythonFunctionWorkflow):
        pass

    @register.register
    def _(self, entity: LaunchPlan):
        pass

    ####################
    # Execute Entities #
    ####################

    @singledispatchmethod
    def execute(self, entity):
        raise NotImplementedError(f"entity type {type(entity)} not recognized for execution")

    # Flyte Remote Entities
    # ---------------------

    @execute.register
    def _(self, entity: FlyteTask):
        pass

    @execute.register
    def _(self, entity: FlyteWorkflow):
        pass

    @execute.register
    def _(self, entity: FlyteWorkflowExecution):
        pass

    @execute.register
    def _(self, entity: FlyteLaunchPlan):
        pass

    # Flytekit Entities
    # -----------------

    @execute.register
    def _(self, entity: PythonFunctionTask):
        pass

    @execute.register
    def _(self, entity: PythonFunctionWorkflow):
        pass

    @execute.register
    def _(self, entity: LaunchPlan):
        pass
