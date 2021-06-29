"""Module defining main Flyte backend entrypoint."""

import typing

try:
    from functools import singledispatchmethod
except ImportError:
    from singledispatchmethod import singledispatchmethod

from flytekit.common.exceptions import scopes as exception_scopes
from flytekit.common.exceptions import user as user_exceptions
from flytekit.core.launch_plan import LaunchPlan
from flytekit.core.python_function_task import PythonFunctionTask
from flytekit.core.workflow import PythonFunctionWorkflow
from flytekit.engines.flyte.engine import get_client
from flytekit.models.admin.common import Sort
from flytekit.models.common import NamedEntityIdentifier
from flytekit.models.core.identifier import ResourceType
from flytekit.remote.identifier import Identifier, WorkflowExecutionIdentifier
from flytekit.remote.launch_plan import FlyteLaunchPlan
from flytekit.remote.tasks.task import FlyteTask
from flytekit.remote.workflow import FlyteWorkflow
from flytekit.remote.workflow_execution import FlyteWorkflowExecution


def _get_latest_version(client_method: typing.Callable, project: str, domain: str, name: str):
    named_entity = NamedEntityIdentifier(project, domain, name)
    entity_list, _ = client_method(
        named_entity,
        limit=1,
        sort_by=Sort("created_at", Sort.Direction.DESCENDING),
    )
    admin_entity = None if not entity_list else entity_list[0]
    if not admin_entity:
        raise user_exceptions.FlyteEntityNotExistException("Named entity {} not found".format(named_entity))
    return admin_entity.id.version


class FlyteRemote(object):
    """Main entrypoint for programmatically accessing Flyte remote backend."""

    def __init__(self):
        # TODO: figure out what config/metadata needs to be loaded into the FlyteRemote object at initialization
        pass

    @exception_scopes.system_entry_point
    def fetch_task(self, project: str, domain: str, name: str, version: str = None) -> FlyteTask:
        client = get_client()
        task_id = Identifier(
            ResourceType.TASK,
            project,
            domain,
            name,
            version if version is not None else _get_latest_version(client.list_tasks_paginated, project, domain, name),
        )
        admin_task = client.get_task(task_id)
        flyte_task = FlyteTask.promote_from_model(admin_task.closure.compiled_task.template)
        flyte_task._id = task_id
        return flyte_task

    @exception_scopes.system_entry_point
    def fetch_workflow(self, project: str, domain: str, name: str, version: str = None) -> FlyteWorkflow:
        client = get_client()
        workflow_id = Identifier(
            ResourceType.WORKFLOW,
            project,
            domain,
            name,
            version
            if version is not None
            else _get_latest_version(client.list_workflows_paginated, project, domain, name),
        )
        admin_workflow = client.get_workflow(workflow_id)
        compiled_wf = admin_workflow.closure.compiled_workflow
        flyte_workflow = FlyteWorkflow.promote_from_model(
            base_model=compiled_wf.primary.template,
            sub_workflows={sw.template.id: sw.template for sw in compiled_wf.sub_workflows},
            tasks={t.template.id: t.template for t in compiled_wf.tasks},
        )
        flyte_workflow._id = workflow_id
        return flyte_workflow

    @exception_scopes.system_entry_point
    def fetch_launch_plan(self, project: str, domain: str, name: str, version: str = None) -> FlyteLaunchPlan:
        client = get_client()
        launch_plan_id = Identifier(
            ResourceType.LAUNCH_PLAN,
            project,
            domain,
            name,
            version
            if version is not None
            else _get_latest_version(client.list_launch_plans_paginated, project, domain, name),
        )

        admin_launch_plan = client.get_launch_plan(launch_plan_id)
        flyte_launch_plan = FlyteLaunchPlan.promote_from_model(admin_launch_plan.spec)
        flyte_launch_plan._id = launch_plan_id

        wf_id = flyte_launch_plan.workflow_id
        workflow = self.fetch_workflow(wf_id.project, wf_id.domain, wf_id.name, wf_id.version)
        flyte_launch_plan._interface = workflow.interface
        return flyte_launch_plan

    @exception_scopes.system_entry_point
    def fetch_workflow_execution(self, project: str, domain: str, name: str) -> FlyteWorkflowExecution:
        return FlyteWorkflowExecution.promote_from_model(
            get_client().get_execution(WorkflowExecutionIdentifier(project, domain, name))
        )

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
