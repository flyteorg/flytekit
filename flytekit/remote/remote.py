"""Module defining main Flyte backend entrypoint."""
from __future__ import annotations

import os
import typing
import uuid
from collections import OrderedDict
from copy import deepcopy
from datetime import datetime, timedelta

from flyteidl.core import literals_pb2 as literals_pb2

from flytekit.clients.friendly import SynchronousFlyteClient
from flytekit.common import utils as common_utils
from flytekit.configuration import platform as platform_config
from flytekit.loggers import remote_logger

try:
    from functools import singledispatchmethod
except ImportError:
    from singledispatchmethod import singledispatchmethod

from flytekit.common.exceptions import user as user_exceptions
from flytekit.common.translator import FlyteControlPlaneEntity, FlyteLocalEntity, get_serializable
from flytekit.configuration import auth as auth_config
from flytekit.configuration.internal import DOMAIN, PROJECT, VERSION
from flytekit.core.base_task import PythonTask
from flytekit.core.context_manager import FlyteContextManager, ImageConfig, SerializationSettings, get_image_config
from flytekit.core.launch_plan import LaunchPlan
from flytekit.core.type_engine import TypeEngine
from flytekit.core.workflow import WorkflowBase
from flytekit.models import common as common_models
from flytekit.models import launch_plan as launch_plan_models
from flytekit.models import literals as literal_models
from flytekit.models.admin.common import Sort
from flytekit.models.core.identifier import ResourceType
from flytekit.models.execution import ExecutionMetadata, ExecutionSpec, NotificationList
from flytekit.remote.identifier import Identifier, WorkflowExecutionIdentifier
from flytekit.remote.launch_plan import FlyteLaunchPlan
from flytekit.remote.tasks.task import FlyteTask
from flytekit.remote.workflow import FlyteWorkflow
from flytekit.remote.workflow_execution import FlyteWorkflowExecution


def _get_latest_version(list_entities_method: typing.Callable, project: str, domain: str, name: str):
    named_entity = common_models.NamedEntityIdentifier(project, domain, name)
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
    resource_type: int,  # from flytekit.models.core.identifier.ResourceType
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


class FlyteRemote(object):
    """Main entrypoint for programmatically accessing Flyte remote backend.

    The term 'remote' is used interchangeably with 'backend' or 'deployment' and refers to a hosted instance of the
    Flyte platform, which comes with a Flyte Admin server on some known URI.

    .. warning::

        This feature is in beta.

    """

    @staticmethod
    def from_environment(
        project: typing.Optional[str] = None, domain: typing.Optional[str] = None, version: typing.Optional[str] = None
    ) -> FlyteRemote:
        project = project or PROJECT.get()
        domain = domain or DOMAIN.get()
        version = version or VERSION.get()
        auth_role = common_models.AuthRole(
            assumable_iam_role=auth_config.ASSUMABLE_IAM_ROLE.get(),
            kubernetes_service_account=auth_config.KUBERNETES_SERVICE_ACCOUNT.get(),
        )

        raw_output_data_prefix = auth_config.RAW_OUTPUT_DATA_PREFIX.get()
        raw_output_data_config = (
            common_models.RawOutputDataConfig(raw_output_data_prefix) if raw_output_data_prefix else None
        )

        image_config = get_image_config()

        return FlyteRemote(
            project=project,
            domain=domain,
            version=version,
            flyte_admin_url=platform_config.URL.get(),
            insecure=platform_config.INSECURE.get(),
            auth_role=auth_role,
            notifications=None,
            labels=None,
            annotations=None,
            image_config=image_config,
            raw_output_data_config=raw_output_data_config,
        )

    def __init__(
        self,
        project: str,
        domain: str,
        version: str,
        flyte_admin_url: str,
        insecure: bool,
        auth_role: typing.Optional[common_models.AuthRole] = None,
        notifications: typing.Optional[typing.List[common_models.Notification]] = None,
        labels: typing.Optional[common_models.Labels] = None,
        annotations: typing.Optional[common_models.Annotations] = None,
        image_config: typing.Optional[ImageConfig] = None,
        raw_output_data_config: typing.Optional[common_models.RawOutputDataConfig] = None,
    ):
        remote_logger.warning("This feature is still in beta. Its interface and UX is subject to change.")

        # TODO: figure out what config/metadata needs to be loaded into the FlyteRemote object at initialization

        self._client = SynchronousFlyteClient(flyte_admin_url, insecure=insecure)

        # - read config files, env vars
        # - host, ssl options for admin client
        self.project = project
        self.domain = domain
        self.version = version
        self.image_config = image_config
        self.auth_role = auth_role
        self.notifications = notifications
        self.labels = labels
        self.annotations = annotations
        self.raw_output_data_config = raw_output_data_config

        # TODO: Reconsider whether we want this. Probably best to not cache.
        self.serialized_entity_cache = OrderedDict()

        self.serialization_settings = SerializationSettings(
            self.project,
            self.domain,
            self.version,
            self.image_config,
        )

    @property
    def client(self) -> SynchronousFlyteClient:
        return self._client

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

    def fetch_task(self, project: str, domain: str, name: str, version: str = None) -> FlyteTask:
        task_id = _get_entity_identifier(
            self.client.list_tasks_paginated, ResourceType.TASK, project, domain, name, version
        )
        admin_task = self.client.get_task(task_id)
        flyte_task = FlyteTask.promote_from_model(admin_task.closure.compiled_task.template)
        flyte_task._id = task_id
        return flyte_task

    def fetch_workflow(self, project: str, domain: str, name: str, version: str = None) -> FlyteWorkflow:
        workflow_id = _get_entity_identifier(
            self.client.list_workflows_paginated, ResourceType.WORKFLOW, project, domain, name, version
        )
        admin_workflow = self.client.get_workflow(workflow_id)
        compiled_wf = admin_workflow.closure.compiled_workflow
        flyte_workflow = FlyteWorkflow.promote_from_model(
            base_model=compiled_wf.primary.template,
            sub_workflows={sw.template.id: sw.template for sw in compiled_wf.sub_workflows},
            tasks={t.template.id: t.template for t in compiled_wf.tasks},
        )
        flyte_workflow._id = workflow_id
        return flyte_workflow

    def fetch_launch_plan(self, project: str, domain: str, name: str, version: str = None) -> FlyteLaunchPlan:
        launch_plan_id = _get_entity_identifier(
            self.client.list_launch_plans_paginated, ResourceType.LAUNCH_PLAN, project, domain, name, version
        )
        admin_launch_plan = self.client.get_launch_plan(launch_plan_id)
        flyte_launch_plan = FlyteLaunchPlan.promote_from_model(admin_launch_plan.spec)
        flyte_launch_plan._id = launch_plan_id

        wf_id = flyte_launch_plan.workflow_id
        workflow = self.fetch_workflow(wf_id.project, wf_id.domain, wf_id.name, wf_id.version)
        flyte_launch_plan._interface = workflow.interface
        return flyte_launch_plan

    def fetch_workflow_execution(self, project: str, domain: str, name: str) -> FlyteWorkflowExecution:
        return FlyteWorkflowExecution.promote_from_model(
            self.client.get_execution(WorkflowExecutionIdentifier(project, domain, name))
        )

    ######################
    # Serialize Entities #
    ######################

    @singledispatchmethod
    def _serialize(self, entity: FlyteLocalEntity) -> FlyteControlPlaneEntity:
        # TODO: Revisit cache
        return get_serializable(self.serialized_entity_cache, self.serialization_settings, entity=entity)

    #####################
    # Register Entities #
    #####################

    @singledispatchmethod
    def register(self, entity):
        raise NotImplementedError(f"entity type {type(entity)} not recognized for registration")

    @register.register
    def _(self, entity: PythonTask):
        self.client.create_task(
            Identifier(ResourceType.TASK, self.project, self.domain, entity.name, self.version),
            task_spec=self._serialize(entity),
        )
        return self.fetch_task(self.project, self.domain, entity.name, self.version)

    @register.register
    def _(self, entity: WorkflowBase):
        self.client.create_workflow(
            Identifier(ResourceType.WORKFLOW, self.project, self.domain, entity.name, self.version),
            workflow_spec=self._serialize(entity),
        )
        return self.fetch_workflow(self.project, self.domain, entity.name, self.version)

    @register.register
    def _(self, entity: LaunchPlan):
        # See _get_patch_launch_plan_fn for what we need to patch. These are the elements of a launch plan
        # that are not set at serialization time and are filled in either by flyte-cli register files or flytectl.
        serialized_lp: launch_plan_models.LaunchPlan = self._serialize(entity)
        serialized_lp.spec._auth_role = common_models.AuthRole(
            self.auth_role.assumable_iam_role, self.auth_role.kubernetes_service_account
        )
        serialized_lp.spec._raw_output_data_config = RawOutputDataConfig(
            self.raw_output_data_config.output_location_prefix
        )

        # Patch in labels and annotations
        if self.labels:
            for k, v in self.labels.values.items():
                serialized_lp.spec._labels.values[k] = v

        if self.annotations:
            for k, v in self.annotations.values.items():
                serialized_lp.spec._annotations.values[k] = v

        self.client.create_launch_plan(
            Identifier(ResourceType.LAUNCH_PLAN, self.project, self.domain, entity.name, self.version),
            launch_plan_spec=serialized_lp.spec,
        )
        return self.fetch_launch_plan(self.project, self.domain, entity.name, self.version)

    ####################
    # Execute Entities #
    ####################

    def _execute(
        self,
        flyte_id: Identifier,
        inputs: typing.Dict[str, typing.Any],
        execution_name: typing.Optional[str] = None,
    ) -> FlyteWorkflowExecution:
        execution_name = execution_name or "f" + uuid.uuid4().hex[:19]
        disable_all = self.notifications == []
        if disable_all:
            notifications = None
        else:
            notifications = NotificationList(self.notifications or [])
            disable_all = None

        literal_inputs = TypeEngine.dict_to_literal_map(FlyteContextManager.current_context(), inputs)
        try:
            exec_id = self.client.create_execution(
                flyte_id.project,
                flyte_id.domain,
                execution_name,
                ExecutionSpec(
                    flyte_id,
                    ExecutionMetadata(
                        ExecutionMetadata.ExecutionMode.MANUAL,
                        "placeholder",  # TODO: get principle
                        0,
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
            exec_id = WorkflowExecutionIdentifier(flyte_id.project, flyte_id.domain, execution_name)
        return FlyteWorkflowExecution.promote_from_model(self.client.get_execution(exec_id))

    @singledispatchmethod
    def execute(self, entity, inputs: typing.Dict[str, typing.Any], execution_name=None):
        raise NotImplementedError(f"entity type {type(entity)} not recognized for execution")

    # Flyte Remote Entities
    # ---------------------

    @execute.register(FlyteTask)
    @execute.register(FlyteLaunchPlan)
    def _(self, entity, inputs: typing.Dict[str, typing.Any], execution_name=None):
        return self._execute(entity.id, inputs, execution_name)

    # @execute.register
    # def _(self, entity: FlyteWorkflow, inputs: typing.Dict[str, typing.Any], execution_name=None):
    #     return self.execute(_create_launch_plan(entity, self.auth_role), inputs, execution_name)

    # Flytekit Entities
    # -----------------

    @execute.register
    def _(self, entity: PythonTask, inputs: typing.Dict[str, typing.Any], execution_name: str = None):
        try:
            flyte_task: FlyteTask = self.fetch_task(self.project, self.domain, entity.name, self.version)
        except Exception:
            # TODO: fast register the task if fast=True
            flyte_task: FlyteTask = self.register(entity)
        return self.execute(flyte_task, inputs, execution_name)

    @execute.register
    def _(self, entity: WorkflowBase, inputs: typing.Dict[str, typing.Any], execution_name=None):
        try:
            flyte_workflow: FlyteWorkflow = self.fetch_workflow(self.project, self.domain, entity.name, self.version)
        except Exception:
            flyte_workflow: FlyteWorkflow = self.register(entity)
        return self.execute(flyte_workflow, inputs, execution_name)

    @execute.register
    def _(self, entity: LaunchPlan, inputs: typing.Dict[str, typing.Any], execution_name=None):
        try:
            flyte_launchplan: FlyteLaunchPlan = self.fetch_launch_plan(
                self.project, self.domain, entity.name, self.version
            )
        except Exception:
            flyte_launchplan: FlyteLaunchPlan = self.register(entity)
        return self.execute(flyte_launchplan, inputs, execution_name)

    @singledispatchmethod
    def wait_for_completion(
        self, execution, timeout: typing.Optional[timedelta] = None, poll_interval: typing.Optional[timedelta] = None
    ):
        raise NotImplementedError(f"Execution type {type(execution)} cannot be waited upon.")

    @wait_for_completion.register
    def _(self, execution: FlyteWorkflowExecution, timeout, poll_interval):
        poll_interval = poll_interval or _datetime.timedelta(seconds=30)
        if timeout is None:
            time_to_give_up = datetime.max
        else:
            time_to_give_up = datetime.utcnow() + timeout

        # TODO: Assess closure for doneness
        # TODO: Async this call and use a timeout
        execution._closure = self.client.get_execution(execution.id).closure

        raise user_exceptions.FlyteTimeout("Execution {} did not complete before timeout.".format(self))

    @singledispatchmethod
    def sync_io(
        self, execution, timeout: typing.Optional[timedelta] = None, poll_interval: typing.Optional[timedelta] = None
    ):
        raise NotImplementedError(f"Execution type {type(execution)} cannot be waited upon.")

    @sync_io.register
    def _(self, execution: FlyteWorkflowExecution):
        execution_data = self.client.get_execution_data(execution.id)

        # Inputs are returned inline unless they are too big, in which case a url blob pointing to them is returned.
        input_map: literal_models.LiteralMap = literal_models.LiteralMap({})
        if bool(execution_data.full_inputs.literals):
            input_map = execution_data.full_inputs
        elif execution_data.inputs.bytes > 0:
            ctx = FlyteContextManager.current_context()
            tmp_name = os.path.join(ctx.file_access.local_sandbox_dir, "inputs.pb")
            ctx.file_access.get_data(execution_data.inputs.url, tmp_name)
            input_map = literal_models.LiteralMap.from_flyte_idl(
                common_utils.load_proto_from_file(literals_pb2.LiteralMap, tmp_name)
            )

        lp_id = execution.spec.launch_plan
        if execution.spec.launch_plan.resource_type == ResourceType.TASK:
            flyte_entity = self.fetch_task(lp_id.project, lp_id.domain, lp_id.name, lp_id.version)
        elif execution.spec.launch_plan.resource_type in {ResourceType.WORKFLOW, ResourceType.LAUNCH_PLAN}:
            flyte_entity = self.fetch_workflow(lp_id.project, lp_id.domain, lp_id.name, lp_id.version)
        else:
            raise user_exceptions.FlyteAssertion(
                f"Resource type {execution.spec.launch_plan.resource_type} not recognized. Must be a TASK or WORKFLOW."
            )

        execution._inputs = TypeEngine.literal_map_to_kwargs(
            ctx=FlyteContextManager.current_context(),
            lm=input_map,
            python_types=TypeEngine.guess_python_types(flyte_entity.interface.inputs),
        )
