"""Module defining main Flyte backend entrypoint."""

import typing
import uuid
from collections import OrderedDict
from copy import deepcopy

try:
    from functools import singledispatchmethod
except ImportError:
    from singledispatchmethod import singledispatchmethod

from flytekit.common.exceptions import scopes as exception_scopes
from flytekit.common.exceptions import user as user_exceptions
from flytekit.common.translator import get_serializable
from flytekit.configuration import auth as auth_config
from flytekit.configuration.internal import DOMAIN, IMAGE, PROJECT, VERSION
from flytekit.core.base_task import PythonTask
from flytekit.core.context_manager import FlyteContext, FlyteContextManager, Image, ImageConfig, SerializationSettings
from flytekit.core.launch_plan import LaunchPlan
from flytekit.core.type_engine import TypeEngine
from flytekit.core.workflow import WorkflowBase
from flytekit.engines.flyte.engine import get_client
from flytekit.models.admin.common import Sort
from flytekit.models.admin.workflow import WorkflowSpec
from flytekit.models.common import Annotations, AuthRole, Labels, NamedEntityIdentifier, RawOutputDataConfig
from flytekit.models.core.identifier import ResourceType
from flytekit.models.execution import ExecutionMetadata, ExecutionSpec, NotificationList
from flytekit.models.interface import ParameterMap
from flytekit.models.launch_plan import LaunchPlanMetadata, LaunchPlanSpec
from flytekit.models.literals import LiteralMap
from flytekit.models.schedule import Schedule
from flytekit.models.task import TaskSpec
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
    """Main entrypoint for programmatically accessing Flyte remote backend.

    The term 'remote' is used interchangeably with 'backend' or 'deployment' and refers to a hosted instance of the
    Flyte platform, which comes with a Flyte Admin server on some known URI.

    .. warning::

        This feature is in beta.

    """

    def __init__(
        self,
        project=None,
        domain=None,
        version=None,
        auth_role=None,
        notifications=None,
        labels=None,
        annotations=None,
    ):
        # TODO: figure out what config/metadata needs to be loaded into the FlyteRemote object at initialization
        # - read config files, env vars
        # - create a Synchronous/Raw client
        # - host, ssl options for admin client
        self.project = project or PROJECT.get()
        self.domain = domain or DOMAIN.get()
        self.version = version or VERSION.get()

        image = IMAGE.get()
        image = "default:tag" if image is None else image
        name, tag = image.split(":")
        self.image: Image = Image(name=name, fqn=image, tag=tag)

        self.auth_role = (
            auth_role
            if auth_role is not None
            else AuthRole(
                assumable_iam_role=auth_config.ASSUMABLE_IAM_ROLE.get(),
                kubernetes_service_account=auth_config.KUBERNETES_SERVICE_ACCOUNT.get(),
            )
        )
        self.notifications = notifications
        self.labels = labels
        self.annotations = annotations

        self.serialized_entity_cache = OrderedDict()

    def with_overrides(
        self,
        project=None,
        domain=None,
        version=None,
        auth_role=None,
        notifications=None,
        labels=None,
        annotations=None,
    ):
        """Create a copy of the remote object, overriding the specified attributes."""
        new_remote = deepcopy(self)
        if project:
            new_remote.project = project
        if domain:
            new_remote.domain = domain
        if version:
            new_remote.version = version
        if auth_role:
            new_remote.auth_role = auth_role
        if notifications:
            new_remote.notifications = notifications
        if labels:
            new_remote.labels = labels
        if annotations:
            new_remote.annotations = annotations
        return new_remote

    def context(self):
        yield from FlyteContext.with_serialization_settings(
            SerializationSettings(
                self.project,
                self.domain,
                self.version,
                ImageConfig(self.image),
            )
        )

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
        with self.context as ctx:
            return get_serializable(self.serialized_entity_cache, ctx.serialization_settings, entity=entity)

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
        get_client().create_task(
            Identifier(ResourceType.TASK, self.project, self.domain, entity.id.name, self.version),
            task_spec=TaskSpec(entity),
        )
        return self.fetch_task(self.project, self.domain, entity.id.name, self.version)

    @register.register
    @exception_scopes.system_entry_point
    def _(self, entity: FlyteWorkflow):
        get_client().create_workflow(
            Identifier(ResourceType.WORKFLOW, self.project, self.domain, entity.id.name, self.version),
            workflow_spec=WorkflowSpec(entity, entity.get_sub_workflows()),
        )
        return self.fetch_workflow(self.project, self.domain, entity.id.name, self.version)

    @register.register
    @exception_scopes.system_entry_point
    def _(self, entity: FlyteLaunchPlan):
        get_client().create_launch_plan(
            Identifier(ResourceType.LAUNCH_PLAN, self.project, self.domain, entity.id.name, self.version),
            launch_plan_spec=entity,
        )
        return self.fetch_launch_plan(self.project, self.domain, entity.id.name, self.version)

    # Flytekit Entities
    # -----------------

    @register.register
    @exception_scopes.system_entry_point
    def _(self, entity: PythonTask):
        get_client().create_task(
            Identifier(ResourceType.TASK, self.project, self.domain, entity.name, self.version),
            task_spec=self._serialize(entity),
        )
        return self.fetch_task(self.project, self.domain, entity.name, self.version)

    @register.register
    @exception_scopes.system_entry_point
    def _(self, entity: WorkflowBase):
        get_client().create_workflow(
            Identifier(ResourceType.WORKFLOW, self.project, self.domain, entity.name, self.version),
            workflow_spec=self._serialize(entity),
        )
        return self.fetch_workflow(self.project, self.domain, entity.name, self.version)

    @register.register
    @exception_scopes.system_entry_point
    def _(self, entity: LaunchPlan):
        get_client().create_launch_plan(
            Identifier(ResourceType.LAUNCH_PLAN, self.project, self.domain, entity.name, self.version),
            launch_plan_spec=self._serialize(entity),
        )
        return self.fetch_launch_plan(self.project, self.domain, entity.name, self.version)

    ####################
    # Execute Entities #
    ####################

    def _execute(
        self,
        id: Identifier,
        inputs: typing.Dict[str, typing.Any],
        execution_name: typing.Optional[str] = None,
    ):
        execution_name = execution_name or "f" + uuid.uuid4().hex[:19]
        disable_all = self.notifications == []
        if disable_all:
            notifications = None
        else:
            notifications = NotificationList(self.notifications or [])
            disable_all = None

        client = get_client()
        literal_inputs = TypeEngine.dict_to_literal_map(FlyteContextManager.current_context(), inputs)
        try:
            exec_id = client.create_execution(
                id.project,
                id.domain,
                execution_name,
                ExecutionSpec(
                    id,
                    ExecutionMetadata(
                        ExecutionMetadata.ExecutionMode.MANUAL,
                        "placeholder",  # TODO: get principle
                        0,  # TODO: Detect nesting
                    ),
                    notifications=notifications,
                    disable_all=disable_all,
                    labels=self.labels,
                    annotations=self.annotations,
                    auth_role=self.auth_role,
                ),
                literal_inputs,
            )
        except user_exceptions.FlyteEntityAlreadyExistsException:
            exec_id = WorkflowExecutionIdentifier(id.project, id.domain, execution_name)
        return FlyteWorkflowExecution.promote_from_model(client.get_execution(exec_id))

    @singledispatchmethod
    @exception_scopes.system_entry_point
    def execute(self, entity, inputs: typing.Dict[str, typing.Any], execution_name=None):
        raise NotImplementedError(f"entity type {type(entity)} not recognized for execution")

    # Flyte Remote Entities
    # ---------------------

    @execute.register(FlyteTask)
    @execute.register(FlyteLaunchPlan)
    @exception_scopes.system_entry_point
    def _(self, entity, inputs: typing.Dict[str, typing.Any], execution_name=None):
        return self._execute(entity.id, inputs, execution_name)

    @execute.register
    @exception_scopes.system_entry_point
    def _(self, entity: FlyteWorkflow, inputs: typing.Dict[str, typing.Any], execution_name=None):
        return self.execute(_create_launch_plan(entity, self.auth_role), inputs, execution_name)

    # Flytekit Entities
    # -----------------

    @execute.register
    @exception_scopes.system_entry_point
    def _(self, entity: PythonTask, inputs: typing.Dict[str, typing.Any], execution_name: str = None):
        try:
            flyte_task: FlyteTask = self.fetch_task(self.project, self.domain, entity.name, self.version)
        except Exception:
            # TODO: fast register the task if fast=True
            flyte_task: FlyteTask = self.register(entity)
        return self.execute(flyte_task, inputs, execution_name)

    @execute.register
    @exception_scopes.system_entry_point
    def _(self, entity: WorkflowBase, inputs: typing.Dict[str, typing.Any], execution_name=None):
        try:
            flyte_workflow: FlyteWorkflow = self.fetch_workflow(self.project, self.domain, entity.name, self.version)
        except Exception:
            flyte_workflow: FlyteWorkflow = self.register(entity)
        return self.execute(flyte_workflow, inputs, execution_name)

    @execute.register
    @exception_scopes.system_entry_point
    def _(self, entity: LaunchPlan, inputs: typing.Dict[str, typing.Any], execution_name=None):
        try:
            flyte_launchplan: FlyteLaunchPlan = self.fetch_launch_plan(
                self.project, self.domain, entity.name, self.version
            )
        except Exception:
            flyte_launchplan: FlyteLaunchPlan = self.register(entity)
        return self.execute(flyte_launchplan, inputs, execution_name)
