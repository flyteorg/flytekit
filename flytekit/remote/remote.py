"""Module defining main Flyte backend entrypoint."""
from __future__ import annotations

import os
import time
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

from flytekit.clients.helpers import iterate_node_executions, iterate_task_executions
from flytekit.common import constants
from flytekit.common.exceptions import user as user_exceptions
from flytekit.common.translator import FlyteControlPlaneEntity, FlyteLocalEntity, get_serializable
from flytekit.configuration import auth as auth_config
from flytekit.configuration.internal import DOMAIN, PROJECT
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
from flytekit.models.execution import (
    ExecutionMetadata,
    ExecutionSpec,
    NodeExecutionGetDataResponse,
    NotificationList,
    WorkflowExecutionGetDataResponse,
)
from flytekit.remote.identifier import Identifier, WorkflowExecutionIdentifier
from flytekit.remote.interface import TypedInterface
from flytekit.remote.launch_plan import FlyteLaunchPlan
from flytekit.remote.nodes import FlyteNode, FlyteNodeExecution
from flytekit.remote.tasks.executions import FlyteTaskExecution
from flytekit.remote.tasks.task import FlyteTask
from flytekit.remote.workflow import FlyteWorkflow
from flytekit.remote.workflow_execution import FlyteWorkflowExecution

ExecutionDataResponse = typing.Union[WorkflowExecutionGetDataResponse, NodeExecutionGetDataResponse]


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
        default_project: typing.Optional[str] = None, default_domain: typing.Optional[str] = None
    ) -> FlyteRemote:
        raw_output_data_prefix = auth_config.RAW_OUTPUT_DATA_PREFIX.get()
        return FlyteRemote(
            default_project=default_project or PROJECT.get(),
            default_domain=default_domain or DOMAIN.get(),
            flyte_admin_url=platform_config.URL.get(),
            insecure=platform_config.INSECURE.get(),
            auth_role=common_models.AuthRole(
                assumable_iam_role=auth_config.ASSUMABLE_IAM_ROLE.get(),
                kubernetes_service_account=auth_config.KUBERNETES_SERVICE_ACCOUNT.get(),
            ),
            notifications=None,
            labels=None,
            annotations=None,
            image_config=get_image_config(),
            raw_output_data_config=(
                common_models.RawOutputDataConfig(raw_output_data_prefix) if raw_output_data_prefix else None
            ),
        )

    def __init__(
        self,
        default_project: str,
        default_domain: str,
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

        self._client = SynchronousFlyteClient(flyte_admin_url, insecure=insecure)

        # read config files, env vars, host, ssl options for admin client
        self.default_project = default_project
        self.default_domain = default_domain
        self.image_config = image_config
        self.auth_role = auth_role
        self.notifications = notifications
        self.labels = labels
        self.annotations = annotations
        self.raw_output_data_config = raw_output_data_config

        # TODO: Reconsider whether we want this. Probably best to not cache.
        self.serialized_entity_cache = OrderedDict()

    @property
    def client(self) -> SynchronousFlyteClient:
        """Return a SynchronousFlyteClient for additional operations."""
        return self._client

    @property
    def version(self) -> str:
        """Get a randomly generated version string."""
        return uuid.uuid4().hex[:30] + str(int(time.time()))

    def with_overrides(
        self,
        default_project: str = None,
        default_domain: str = None,
        flyte_admin_url: str = None,
        insecure: bool = None,
        auth_role: typing.Optional[common_models.AuthRole] = None,
        notifications: typing.Optional[typing.List[common_models.Notification]] = None,
        labels: typing.Optional[common_models.Labels] = None,
        annotations: typing.Optional[common_models.Annotations] = None,
        image_config: typing.Optional[ImageConfig] = None,
        raw_output_data_config: typing.Optional[common_models.RawOutputDataConfig] = None,
    ):
        """Create a copy of the remote object, overriding the specified attributes."""
        new_remote = deepcopy(self)
        if default_project:
            new_remote.default_project = default_project
        if default_domain:
            new_remote.default_domain = default_domain
        if flyte_admin_url:
            new_remote.flyte_admin_url = flyte_admin_url
        if insecure:
            new_remote.insecure = insecure
        if auth_role:
            new_remote.auth_role = auth_role
        if notifications:
            new_remote.notifications = notifications
        if labels:
            new_remote.labels = labels
        if annotations:
            new_remote.annotations = annotations
        if image_config:
            new_remote.image_config = image_config
        if raw_output_data_config:
            new_remote.raw_output_data_config = raw_output_data_config
        return new_remote

    def fetch_task(self, project: str = None, domain: str = None, name: str = None, version: str = None) -> FlyteTask:
        if name is None:
            raise user_exceptions.FlyteAssertion("the 'name' argument must be specified.")
        task_id = _get_entity_identifier(
            self.client.list_tasks_paginated,
            ResourceType.TASK,
            project or self.default_project,
            domain or self.default_domain,
            name,
            version,
        )
        admin_task = self.client.get_task(task_id)
        flyte_task = FlyteTask.promote_from_model(admin_task.closure.compiled_task.template)
        flyte_task._id = task_id
        return flyte_task

    def fetch_workflow(
        self, project: str = None, domain: str = None, name: str = None, version: str = None
    ) -> FlyteWorkflow:
        if name is None:
            raise user_exceptions.FlyteAssertion("the 'name' argument must be specified.")
        workflow_id = _get_entity_identifier(
            self.client.list_workflows_paginated,
            ResourceType.WORKFLOW,
            project or self.default_project,
            domain or self.default_domain,
            name,
            version,
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

    def fetch_launch_plan(
        self, project: str = None, domain: str = None, name: str = None, version: str = None
    ) -> FlyteLaunchPlan:
        if name is None:
            raise user_exceptions.FlyteAssertion("the 'name' argument must be specified.")
        launch_plan_id = _get_entity_identifier(
            self.client.list_launch_plans_paginated,
            ResourceType.LAUNCH_PLAN,
            project or self.default_project,
            domain or self.default_domain,
            name,
            version,
        )
        admin_launch_plan = self.client.get_launch_plan(launch_plan_id)
        flyte_launch_plan = FlyteLaunchPlan.promote_from_model(admin_launch_plan.spec)
        flyte_launch_plan._id = launch_plan_id

        wf_id = flyte_launch_plan.workflow_id
        workflow = self.fetch_workflow(wf_id.project, wf_id.domain, wf_id.name, wf_id.version)
        flyte_launch_plan._interface = workflow.interface
        return flyte_launch_plan

    def fetch_workflow_execution(
        self, project: str = None, domain: str = None, name: str = None
    ) -> FlyteWorkflowExecution:
        if name is None:
            raise user_exceptions.FlyteAssertion("the 'name' argument must be specified.")
        return FlyteWorkflowExecution.promote_from_model(
            self.client.get_execution(
                WorkflowExecutionIdentifier(
                    project or self.default_project,
                    domain or self.default_domain,
                    name,
                )
            )
        )

    ######################
    # Serialize Entities #
    ######################

    @singledispatchmethod
    def _serialize(
        self,
        entity: FlyteLocalEntity,
        project: str = None,
        domain: str = None,
        version: str = None,
        **kwargs,
    ) -> FlyteControlPlaneEntity:
        # TODO: Revisit cache
        return get_serializable(
            self.serialized_entity_cache,
            SerializationSettings(
                project or self.default_project,
                domain or self.default_domain,
                version or self.version,
                self.image_config,
            ),
            entity=entity,
        )

    #####################
    # Register Entities #
    #####################

    def _resolve_identifier_kwargs(
        self,
        entity,
        project: typing.Optional[str],
        domain: typing.Optional[str],
        name: typing.Optional[str],
        version: typing.Optional[str],
    ):
        """Resolves the identifier attributes based on user input, falling back on ."""
        return {
            "project": project or self.default_project,
            "domain": domain or self.default_domain,
            "name": name or entity.name,
            "version": version or self.version,
        }

    @singledispatchmethod
    def register(self, entity, project: str = None, domain: str = None, name: str = None, version: str = None):
        """Register an entity to flyte admin."""
        raise NotImplementedError(f"entity type {type(entity)} not recognized for registration")

    @register.register
    def _(self, entity: PythonTask, project: str = None, domain: str = None, name: str = None, version: str = None):
        """Register an @task-decorated function or TaskTemplate task to flyte admin."""
        flyte_id_kwargs = self._resolve_identifier_kwargs(entity, project, domain, name, version)
        self.client.create_task(
            Identifier(ResourceType.TASK, **flyte_id_kwargs),
            task_spec=self._serialize(entity, **flyte_id_kwargs),
        )
        return self.fetch_task(**flyte_id_kwargs)

    @register.register
    def _(self, entity: WorkflowBase, project: str = None, domain: str = None, name: str = None, version: str = None):
        """Register an @workflow-decorated function to flyte admin."""
        flyte_id_kwargs = self._resolve_identifier_kwargs(entity, project, domain, name, version)
        self.client.create_workflow(
            Identifier(ResourceType.WORKFLOW, **flyte_id_kwargs),
            workflow_spec=self._serialize(entity, **flyte_id_kwargs),
        )
        return self.fetch_workflow(**flyte_id_kwargs)

    @register.register
    def _(self, entity: LaunchPlan, project: str = None, domain: str = None, name: str = None, version: str = None):
        """Register a LaunchPlan object to flyte admin."""
        # See _get_patch_launch_plan_fn for what we need to patch. These are the elements of a launch plan
        # that are not set at serialization time and are filled in either by flyte-cli register files or flytectl.
        flyte_id_kwargs = self._resolve_identifier_kwargs(entity, project, domain, name, version)
        serialized_lp: launch_plan_models.LaunchPlan = self._serialize(entity, **flyte_id_kwargs)
        if self.auth_role:
            serialized_lp.spec._auth_role = common_models.AuthRole(
                self.auth_role.assumable_iam_role, self.auth_role.kubernetes_service_account
            )
        if self.raw_output_data_config:
            serialized_lp.spec._raw_output_data_config = common_models.RawOutputDataConfig(
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
            Identifier(ResourceType.LAUNCH_PLAN, **flyte_id_kwargs),
            launch_plan_spec=serialized_lp.spec,
        )
        return self.fetch_launch_plan(**flyte_id_kwargs)

    ####################
    # Execute Entities #
    ####################

    def _execute(
        self,
        flyte_id: Identifier,
        inputs: typing.Dict[str, typing.Any],
        execution_name: typing.Optional[str] = None,
        wait: bool = False,
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
        execution = FlyteWorkflowExecution.promote_from_model(self.client.get_execution(exec_id))
        if wait:
            return self.wait(execution)
        return execution

    @singledispatchmethod
    def execute(
        self,
        entity,
        inputs: typing.Dict[str, typing.Any],
        execution_name=None,
        project: str = None,
        domain: str = None,
        version: str = None,
        wait=False,
    ) -> FlyteWorkflowExecution:
        """Execute a task, workflow, or launchplan."""
        raise NotImplementedError(f"entity type {type(entity)} not recognized for execution")

    # Flyte Remote Entities
    # ---------------------

    @execute.register(FlyteTask)
    @execute.register(FlyteLaunchPlan)
    def _(
        self,
        entity,
        inputs: typing.Dict[str, typing.Any],
        project: str = None,
        domain: str = None,
        name: str = None,
        version: str = None,
        execution_name=None,
        wait=False,
    ) -> FlyteWorkflowExecution:
        """Execute a FlyteTask, or FlyteLaunchplan."""
        return self._execute(entity.id, inputs, execution_name, wait)

    @execute.register
    def _(
        self,
        entity: FlyteWorkflow,
        inputs: typing.Dict[str, typing.Any],
        project: str = None,
        domain: str = None,
        name: str = None,
        version: str = None,
        execution_name=None,
        wait=False,
    ) -> FlyteWorkflowExecution:
        """Execute a FlyteWorkflow."""
        return self.execute(
            self.fetch_launch_plan(entity.id.project, entity.id.domain, entity.id.name, entity.id.version),
            inputs,
            execution_name=execution_name,
            wait=wait,
        )

    # Flytekit Entities
    # -----------------

    @execute.register
    def _(
        self,
        entity: PythonTask,
        inputs: typing.Dict[str, typing.Any],
        project: str = None,
        domain: str = None,
        name: str = None,
        version: str = None,
        execution_name: str = None,
        wait=False,
    ) -> FlyteWorkflowExecution:
        """Execute an @task-decorated function or TaskTemplate task."""
        flyte_id_kwargs = self._resolve_identifier_kwargs(entity, project, domain, name, version)
        try:
            flyte_task: FlyteTask = self.fetch_task(**flyte_id_kwargs)
        except Exception:
            flyte_task: FlyteTask = self.register(entity, **flyte_id_kwargs)
        return self.execute(flyte_task, inputs, execution_name=execution_name, wait=wait)

    @execute.register
    def _(
        self,
        entity: WorkflowBase,
        inputs: typing.Dict[str, typing.Any],
        project: str = None,
        domain: str = None,
        name: str = None,
        version: str = None,
        execution_name=None,
        wait=False,
    ) -> FlyteWorkflowExecution:
        """Execute an @workflow-decorated function."""
        flyte_id_kwargs = self._resolve_identifier_kwargs(entity, project, domain, name, version)
        try:
            flyte_workflow: FlyteWorkflow = self.fetch_workflow(**flyte_id_kwargs)
        except Exception:
            flyte_workflow: FlyteWorkflow = self.register(entity, **flyte_id_kwargs)
        return self.execute(flyte_workflow, inputs, execution_name=execution_name, wait=wait)

    @execute.register
    def _(
        self,
        entity: LaunchPlan,
        inputs: typing.Dict[str, typing.Any],
        project: str = None,
        domain: str = None,
        name: str = None,
        version: str = None,
        execution_name=None,
        wait=False,
    ) -> FlyteWorkflowExecution:
        """Execute a LaunchPlan object."""
        flyte_id_kwargs = self._resolve_identifier_kwargs(entity, project, domain, name, version)
        try:
            flyte_launchplan: FlyteLaunchPlan = self.fetch_launch_plan(**flyte_id_kwargs)
        except Exception:
            flyte_launchplan: FlyteLaunchPlan = self.register(entity, **flyte_id_kwargs)
        return self.execute(flyte_launchplan, inputs, execution_name=execution_name, wait=wait)

    ###################################
    # Wait for Executions to Complete #
    ###################################

    @singledispatchmethod
    def wait(
        self,
        execution,
        timeout: typing.Optional[timedelta] = None,
        poll_interval: typing.Optional[timedelta] = None,
    ):
        """Wait for an execution to finish."""
        raise NotImplementedError(f"Execution type {type(execution)} cannot be waited upon.")

    @wait.register
    def _(
        self,
        execution: FlyteWorkflowExecution,
        timeout: typing.Optional[timedelta] = None,
        poll_interval: typing.Optional[timedelta] = None,
    ):
        """Wait for a FlyteWorkflowExecution to finish."""
        poll_interval = poll_interval or timedelta(seconds=30)
        time_to_give_up = datetime.max if timeout is None else datetime.utcnow() + timeout

        while datetime.utcnow() < time_to_give_up:
            execution = self.sync(execution)
            if execution.is_complete:
                return execution
            time.sleep(poll_interval.total_seconds())

        raise user_exceptions.FlyteTimeout(f"Execution {self} did not complete before timeout.")

    ########################
    # Sync Execution State #
    ########################

    @singledispatchmethod
    def sync(self, execution):
        """Sync a flyte execution object with its corresponding remote state."""
        raise NotImplementedError(f"Execution type {type(execution)} cannot be synced.")

    @sync.register
    def _(self, execution: FlyteWorkflowExecution) -> FlyteWorkflowExecution:
        """Sync a FlyteWorkflowExecution object with its corresponding remote state."""
        execution_data = self.client.get_execution_data(execution.id)
        lp_id = execution.spec.launch_plan
        if execution.spec.launch_plan.resource_type == ResourceType.TASK:
            flyte_entity = self.fetch_task(lp_id.project, lp_id.domain, lp_id.name, lp_id.version)
        elif execution.spec.launch_plan.resource_type in {ResourceType.WORKFLOW, ResourceType.LAUNCH_PLAN}:
            flyte_entity = self.fetch_workflow(lp_id.project, lp_id.domain, lp_id.name, lp_id.version)
        else:
            raise user_exceptions.FlyteAssertion(
                f"Resource type {execution.spec.launch_plan.resource_type} not recognized. Must be a TASK or WORKFLOW."
            )

        synced_execution = deepcopy(execution)
        # sync closure, node executions, and inputs/outputs
        synced_execution._closure = self.client.get_execution(execution.id).closure
        synced_execution._node_executions = {
            node.id.node_id: self.sync(FlyteNodeExecution.promote_from_model(node))
            for node in iterate_node_executions(self.client, execution.id)
        }
        return self._assign_inputs_and_outputs(synced_execution, execution_data, flyte_entity.interface)

    @sync.register
    def _(self, execution: FlyteNodeExecution) -> FlyteNodeExecution:
        """Sync a FlyteNodeExecution object with its corresponding remote state."""

        if (
            execution.id.node_id in {constants.START_NODE_ID, constants.END_NODE_ID}
            or execution.id.node_id.endswith(constants.START_NODE_ID)
            or execution.id.node_id.endswith(constants.END_NODE_ID)
        ):
            return execution

        synced_execution = deepcopy(execution)

        # sync closure, child nodes, interface, and inputs/outputs
        synced_execution._closure = self.client.get_node_execution(execution.id).closure
        if synced_execution.metadata.is_parent_node:
            synced_execution._subworkflow_node_executions = [
                self.sync(FlyteNodeExecution.promote_from_model(node))
                for node in iterate_node_executions(
                    self.client,
                    workflow_execution_identifier=synced_execution.id.execution_id,
                    unique_parent_id=synced_execution.id.node_id,
                )
            ]
        else:
            synced_execution._task_executions = [
                self.sync(FlyteTaskExecution.promote_from_model(t))
                for t in iterate_task_executions(self.client, synced_execution.id)
            ]
        synced_execution._interface = self._get_node_execution_interface(synced_execution)
        return self._assign_inputs_and_outputs(
            synced_execution,
            self.client.get_node_execution_data(execution.id),
            synced_execution.interface,
        )

    @sync.register
    def _(self, execution: FlyteTaskExecution) -> FlyteTaskExecution:
        """Sync a FlyteTaskExecution object with its corresponding remote state."""
        synced_execution = deepcopy(execution)

        # sync closure and inputs/outputs
        synced_execution._closure = self.client.get_task_execution(synced_execution.id).closure
        execution_data = self.client.get_task_execution_data(synced_execution.id)
        task_id = execution.id.task_id
        task = self.fetch_task(task_id.project, task_id.domain, task_id.name, task_id.version)
        return self._assign_inputs_and_outputs(synced_execution, execution_data, task.interface)

    def _assign_inputs_and_outputs(self, execution, execution_data, interface):
        execution._inputs = TypeEngine.literal_map_to_kwargs(
            ctx=FlyteContextManager.current_context(),
            lm=self._get_input_literal_map(execution_data),
            python_types=TypeEngine.guess_python_types(interface.inputs),
        )
        if execution.is_complete and not execution.error:
            execution._outputs = TypeEngine.literal_map_to_kwargs(
                ctx=FlyteContextManager.current_context(),
                lm=self._get_output_literal_map(execution_data),
                python_types=TypeEngine.guess_python_types(interface.outputs),
            )
        return execution

    def _get_input_literal_map(self, execution_data: ExecutionDataResponse) -> literal_models.LiteralMap:
        # Inputs are returned inline unless they are too big, in which case a url blob pointing to them is returned.
        if bool(execution_data.full_inputs.literals):
            return execution_data.full_inputs
        elif execution_data.inputs.bytes > 0:
            ctx = FlyteContextManager.current_context()
            tmp_name = os.path.join(ctx.file_access.local_sandbox_dir, "inputs.pb")
            ctx.file_access.get_data(execution_data.inputs.url, tmp_name)
            return literal_models.LiteralMap.from_flyte_idl(
                common_utils.load_proto_from_file(literals_pb2.LiteralMap, tmp_name)
            )
        return literal_models.LiteralMap({})

    def _get_output_literal_map(self, execution_data: ExecutionDataResponse) -> literal_models.LiteralMap:
        # Outputs are returned inline unless they are too big, in which case a url blob pointing to them is returned.
        if bool(execution_data.full_outputs.literals):
            return execution_data.full_outputs
        elif execution_data.outputs.bytes > 0:
            ctx = FlyteContextManager.current_context()
            tmp_name = os.path.join(ctx.file_access.local_sandbox_dir, "outputs.pb")
            ctx.file_access.get_data(execution_data.outputs.url, tmp_name)
            return literal_models.LiteralMap.from_flyte_idl(
                common_utils.load_proto_from_file(literals_pb2.LiteralMap, tmp_name)
            )
        return literal_models.LiteralMap({})

    def _get_node_execution_interface(self, node_execution: FlyteNodeExecution) -> TypedInterface:
        """
        Return the interface of the task or subworkflow associated with this node execution.
        """
        if not node_execution.metadata.is_parent_node:
            # if not a parent node, assume a task execution node
            task_id = node_execution.task_executions[0].id.task_id
            task = self.fetch_task(task_id.project, task_id.domain, task_id.name, task_id.version)
            return task.interface

        # otherwise assume the node is associated with a subworkflow
        # need to get the FlyteWorkflow associated with the node execution (self), so we need to fetch the
        # parent workflow and iterate through the parent's FlyteNodes to get the the FlyteWorkflow object
        # representing the subworkflow. This allows us to get the interface for guessing the types of the
        # inputs/outputs.
        lp_id = self.client.get_execution(node_execution.id.execution_id).spec.launch_plan
        workflow = self.fetch_workflow(lp_id.project, lp_id.domain, lp_id.name, lp_id.version)
        flyte_subworkflow_node: FlyteNode = [n for n in workflow.nodes if n.id == node_execution.id.node_id][0]
        return flyte_subworkflow_node.target.flyte_workflow.interface
