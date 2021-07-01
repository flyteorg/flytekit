"""Module defining main Flyte backend entrypoint."""

import typing
import uuid

try:
    from functools import singledispatchmethod
except ImportError:
    from singledispatchmethod import singledispatchmethod

from flytekit.common.exceptions import scopes as exception_scopes
from flytekit.common.exceptions import user as user_exceptions
from flytekit.configuration import auth as auth_config
from flytekit.core.context_manager import FlyteContextManager
from flytekit.core.launch_plan import LaunchPlan
from flytekit.core.python_customized_container_task import PythonCustomizedContainerTask
from flytekit.core.python_function_task import PythonFunctionTask
from flytekit.core.type_engine import TypeEngine
from flytekit.core.workflow import PythonFunctionWorkflow
from flytekit.engines.flyte.engine import get_client
from flytekit.models.admin.common import Sort
from flytekit.models.common import Annotations, AuthRole, Labels, NamedEntityIdentifier, RawOutputDataConfig
from flytekit.models.core.identifier import ResourceType
from flytekit.models.execution import ExecutionMetadata, ExecutionSpec, NotificationList
from flytekit.models.interface import ParameterMap
from flytekit.models.launch_plan import LaunchPlanMetadata
from flytekit.models.literals import LiteralMap
from flytekit.models.schedule import Schedule
from flytekit.remote.identifier import Identifier, WorkflowExecutionIdentifier
from flytekit.remote.launch_plan import FlyteLaunchPlan
from flytekit.remote.tasks.task import FlyteTask
from flytekit.remote.workflow import FlyteWorkflow
from flytekit.remote.workflow_execution import FlyteWorkflowExecution


def _get_latest_version(list_entities_method: typing.Callable, project: str, domain: str, name: str):
    named_entity = NamedEntityIdentifier(project, domain, name)
    entity_list, _ = list_entities_method(
        named_entity,
        limit=1,
        sort_by=Sort("created_at", Sort.Direction.DESCENDING),
    )
    admin_entity = None if not entity_list else entity_list[0]
    if not admin_entity:
        raise user_exceptions.FlyteEntityNotExistException("Named entity {} not found".format(named_entity))
    return admin_entity.id.version


def _get_entity_identifier(
    list_entities_method: typing.Callable,
    resource_type: ResourceType,
    project: str,
    domain: str,
    name: str,
    version: typing.Optional[str] = None,
):
    return Identifier(
        resource_type,
        project,
        domain,
        name,
        version if version is not None else _get_latest_version(list_entities_method, project, domain, name),
    )


def _execute(
    identifier: Identifier,
    inputs: typing.Dict[str, typing.Any],
    name: typing.Optional[str] = None,
    notification_overrides=None,
    label_overrides=None,
    annotation_overrides=None,
    auth_role=None,
):
    name = name or "f" + uuid.uuid4().hex[:19]
    disable_all = notification_overrides == []
    if disable_all:
        notification_overrides = None
    else:
        notification_overrides = NotificationList(notification_overrides or [])
        disable_all = None

    client = get_client()
    literal_inputs = TypeEngine.dict_to_literal_map(FlyteContextManager.current_context(), inputs)
    try:
        exec_id = client.create_execution(
            identifier.project,
            identifier.domain,
            name,
            ExecutionSpec(
                identifier,
                ExecutionMetadata(
                    ExecutionMetadata.ExecutionMode.MANUAL,
                    "placeholder",  # TODO: get principle
                    0,  # TODO: Detect nesting
                ),
                notifications=notification_overrides,
                disable_all=disable_all,
                labels=label_overrides,
                annotations=annotation_overrides,
                auth_role=auth_role,
            ),
            literal_inputs,
        )
    except user_exceptions.FlyteEntityAlreadyExistsException:
        exec_id = WorkflowExecutionIdentifier(identifier.project, identifier.domain, name)
    return FlyteWorkflowExecution.promote_from_model(client.get_execution(exec_id))


@exception_scopes.system_entry_point
def _create_launch_plan(workflow: FlyteWorkflow, auth_role: AuthRole):
    launch_plan = FlyteLaunchPlan(
        workflow_id=workflow.id,
        entity_metadata=LaunchPlanMetadata(schedule=Schedule(""), notifications=[]),
        default_inputs=ParameterMap({}),
        fixed_inputs=LiteralMap(literals={}),
        labels=Labels({}),
        annotations=Annotations({}),
        auth_role=auth_role,
        raw_output_data_config=RawOutputDataConfig(""),
    )
    launch_plan._id = Identifier(
        ResourceType.LAUNCH_PLAN, workflow.id.project, workflow.id.domain, workflow.id.name, workflow.id.version
    )
    return launch_plan


class FlyteRemote(object):
    """Main entrypoint for programmatically accessing Flyte remote backend."""

    def __init__(self):
        # TODO: figure out what config/metadata needs to be loaded into the FlyteRemote object at initialization
        # - read config files, env vars
        # - create a Synchronous/Raw client
        pass

    @exception_scopes.system_entry_point
    def fetch_task(self, project: str, domain: str, name: str, version: str = None) -> FlyteTask:
        client = get_client()
        task_id = _get_entity_identifier(client.list_tasks_paginated, ResourceType.TASK, project, domain, name, version)
        admin_task = client.get_task(task_id)
        flyte_task = FlyteTask.promote_from_model(admin_task.closure.compiled_task.template)
        flyte_task._id = task_id
        return flyte_task

    @exception_scopes.system_entry_point
    def fetch_workflow(self, project: str, domain: str, name: str, version: str = None) -> FlyteWorkflow:
        client = get_client()
        workflow_id = _get_entity_identifier(
            client.list_workflows_paginated, ResourceType.WORKFLOW, project, domain, name, version
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
        launch_plan_id = _get_entity_identifier(
            client.list_launch_plans_paginated, ResourceType.LAUNCH_PLAN, project, domain, name, version
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

    ######################
    # Serialize Entities #
    ######################

    @singledispatchmethod
    @exception_scopes.system_entry_point
    def _serialize(self, entity):
        raise NotImplementedError(f"entity type {type(entity)} not recognized for serialization")

    # Flyte Remote Entities
    # ---------------------
    @_serialize.register
    @exception_scopes.system_entry_point
    def _(self, entity: FlyteTask):
        pass

    @_serialize.register
    @exception_scopes.system_entry_point
    def _(self, entity: FlyteWorkflow):
        pass

    @_serialize.register
    @exception_scopes.system_entry_point
    def _(self, entity: FlyteWorkflowExecution):
        pass

    @_serialize.register
    @exception_scopes.system_entry_point
    def _(self, entity: FlyteLaunchPlan):
        pass

    # Flytekit Entities
    # -----------------
    @_serialize.register
    @exception_scopes.system_entry_point
    def _(self, entity: PythonFunctionTask):
        pass

    @_serialize.register
    @exception_scopes.system_entry_point
    def _(self, entity: PythonFunctionWorkflow):
        pass

    @_serialize.register
    @exception_scopes.system_entry_point
    def _(self, entity: LaunchPlan):
        pass

    @_serialize.register
    @exception_scopes.system_entry_point
    def _(self, entity: PythonCustomizedContainerTask):
        pass

    #####################
    # Register Entities #
    #####################

    @singledispatchmethod
    @exception_scopes.system_entry_point
    def register(self, entity):
        raise NotImplementedError(f"entity type {type(entity)} not recognized for registration")

    # Flyte Remote Entities
    # ---------------------

    # TODO: it may not make sense to register Flyte* objects since these are already assumed to be registered in the
    # relevant backend? Is there a use case of e.g. fetching a FlyteWorkflow, modifying it somehow, and re-registering
    # it under a new project/domain/name?

    @register.register
    @exception_scopes.system_entry_point
    def _(self, entity: FlyteTask):
        pass

    @register.register
    @exception_scopes.system_entry_point
    def _(self, entity: FlyteWorkflow):
        pass

    @register.register
    @exception_scopes.system_entry_point
    def _(self, entity: FlyteWorkflowExecution):
        pass

    @register.register
    @exception_scopes.system_entry_point
    def _(self, entity: FlyteLaunchPlan):
        pass

    # Flytekit Entities
    # -----------------

    @register.register
    @exception_scopes.system_entry_point
    def _(self, entity: PythonFunctionTask):
        pass

    @register.register
    @exception_scopes.system_entry_point
    def _(self, entity: PythonFunctionWorkflow):
        pass

    @register.register
    @exception_scopes.system_entry_point
    def _(self, entity: LaunchPlan):
        pass

    @register.register
    @exception_scopes.system_entry_point
    def _(self, entity: PythonCustomizedContainerTask):
        pass

    ####################
    # Execute Entities #
    ####################

    @singledispatchmethod
    @exception_scopes.system_entry_point
    def execute(
        self,
        entity,
        inputs,
        name=None,
        notification_overrides=None,
        label_overrides=None,
        annotation_overrides=None,
        auth_role=None,
    ):
        raise NotImplementedError(f"entity type {type(entity)} not recognized for execution")

    # Flyte Remote Entities
    # ---------------------

    @execute.register
    @exception_scopes.system_entry_point
    def _(
        self,
        entity: FlyteTask,
        inputs,
        name=None,
        notification_overrides=None,
        label_overrides=None,
        annotation_overrides=None,
        auth_role=None,
    ):
        return _execute(
            entity.id,
            inputs,
            name,
            notification_overrides,
            label_overrides,
            annotation_overrides,
            # Unlike regular workflow executions, single task executions must always specify an auth role, since there
            # isn't any existing launch plan with a bound auth role to fall back on.
            (
                AuthRole(
                    assumable_iam_role=auth_config.ASSUMABLE_IAM_ROLE.get(),
                    kubernetes_service_account=auth_config.KUBERNETES_SERVICE_ACCOUNT.get(),
                )
                if auth_role is None
                else auth_role
            ),
        )

    @execute.register
    @exception_scopes.system_entry_point
    def _(
        self,
        entity: FlyteWorkflow,
        inputs,
        name=None,
        notification_overrides=None,
        label_overrides=None,
        annotation_overrides=None,
        auth_role=None,
    ):
        return self.execute(
            _create_launch_plan(entity, auth_role),
            inputs,
            name,
            notification_overrides,
            label_overrides,
            annotation_overrides,
            auth_role,
        )

    @execute.register
    @exception_scopes.system_entry_point
    def _(
        self,
        entity: FlyteLaunchPlan,
        inputs,
        name=None,
        notification_overrides=None,
        label_overrides=None,
        annotation_overrides=None,
        auth_role=None,
    ):
        return _execute(
            entity.id, inputs, name, notification_overrides, label_overrides, annotation_overrides, auth_role
        )

    # Flytekit Entities
    # -----------------

    @execute.register
    @exception_scopes.system_entry_point
    def _(self, entity: PythonFunctionTask):
        pass

    @execute.register
    @exception_scopes.system_entry_point
    def _(self, entity: PythonFunctionWorkflow):
        pass

    @execute.register
    @exception_scopes.system_entry_point
    def _(self, entity: LaunchPlan):
        pass

    @execute.register
    @exception_scopes.system_entry_point
    def _(self, entity: PythonCustomizedContainerTask):
        pass
