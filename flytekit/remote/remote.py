"""Module defining main Flyte backend entrypoint."""
from __future__ import annotations

import os
import time
import typing
import uuid
from collections import OrderedDict
from copy import deepcopy
from dataclasses import asdict, dataclass
from datetime import datetime, timedelta

from flyteidl.core import literals_pb2 as literals_pb2

from flytekit.clients.friendly import SynchronousFlyteClient
from flytekit.common import utils as common_utils
from flytekit.configuration import platform as platform_config
from flytekit.configuration import sdk as sdk_config
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
from flytekit.interfaces.data.data_proxy import FileAccessProvider
from flytekit.interfaces.data.gcs.gcs_proxy import GCSProxy
from flytekit.interfaces.data.s3.s3proxy import AwsS3Proxy
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
from flytekit.remote.nodes import FlyteNodeExecution
from flytekit.remote.tasks.executions import FlyteTaskExecution
from flytekit.remote.tasks.task import FlyteTask
from flytekit.remote.workflow import FlyteWorkflow
from flytekit.remote.workflow_execution import FlyteWorkflowExecution

ExecutionDataResponse = typing.Union[WorkflowExecutionGetDataResponse, NodeExecutionGetDataResponse]


@dataclass
class ResolvedIdentifiers:
    project: str
    domain: str
    name: str
    version: str


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
    """Main entrypoint for programmatically accessing a Flyte remote backend.

    The term 'remote' is synonymous with 'backend' or 'deployment' and refers to a hosted instance of the
    Flyte platform, which comes with a Flyte Admin server on some known URI.

    .. warning::

        This feature is in beta.

    """

    @classmethod
    def from_config(
        cls, default_project: typing.Optional[str] = None, default_domain: typing.Optional[str] = None
    ) -> FlyteRemote:
        """Create a FlyteRemote object using flyte configuration variables and/or environment variable overrides.

        :param default_project: default project to use when fetching or executing flyte entities.
        :param default_domain: default domain to use when fetching or executing flyte entities.
        """
        raw_output_data_prefix = auth_config.RAW_OUTPUT_DATA_PREFIX.get()
        raw_output_data_prefix = raw_output_data_prefix if raw_output_data_prefix else None

        return cls(
            flyte_admin_url=platform_config.URL.get(),
            insecure=platform_config.INSECURE.get(),
            default_project=default_project or PROJECT.get() or None,
            default_domain=default_domain or DOMAIN.get() or None,
            file_access=FileAccessProvider(
                local_sandbox_dir=sdk_config.LOCAL_SANDBOX.get(),
                remote_proxy={
                    constants.CloudProvider.AWS: AwsS3Proxy(raw_output_data_prefix),
                    constants.CloudProvider.GCP: GCSProxy(raw_output_data_prefix),
                    constants.CloudProvider.LOCAL: None,
                }.get(platform_config.CLOUD_PROVIDER.get(), None),
            ),
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
        flyte_admin_url: str,
        insecure: bool,
        default_project: typing.Optional[str] = None,
        default_domain: typing.Optional[str] = None,
        file_access: typing.Optional[FileAccessProvider] = None,
        auth_role: typing.Optional[common_models.AuthRole] = None,
        notifications: typing.Optional[typing.List[common_models.Notification]] = None,
        labels: typing.Optional[common_models.Labels] = None,
        annotations: typing.Optional[common_models.Annotations] = None,
        image_config: typing.Optional[ImageConfig] = None,
        raw_output_data_config: typing.Optional[common_models.RawOutputDataConfig] = None,
    ):
        """Initilize a FlyteRemote object.

        :param flyte_admin_url: url pointing to the remote backend.
        :param insecure: whether or not the enable SSL.
        :param default_project: default project to use when fetching or executing flyte entities.
        :param default_domain: default domain to use when fetching or executing flyte entities.
        :param file_access: file access provider to use for offloading non-literal inputs/outputs.
        :param auth_role: auth role config
        :param notifications: notification config
        :param labels: label config
        :param annotations: annotation config
        :param image_config: image config
        :param raw_output_data_config: location for offloaded data, e.g. in S3
        """
        remote_logger.warning("This feature is still in beta. Its interface and UX is subject to change.")

        self._client = SynchronousFlyteClient(flyte_admin_url, insecure=insecure)

        # read config files, env vars, host, ssl options for admin client
        self._flyte_admin_url = flyte_admin_url
        self._insecure = insecure
        self._default_project = default_project
        self._default_domain = default_domain
        self._image_config = image_config
        self._file_access = file_access
        self._auth_role = auth_role
        self._notifications = notifications
        self._labels = labels
        self._annotations = annotations
        self._raw_output_data_config = raw_output_data_config

        # TODO: Reconsider whether we want this. Probably best to not cache.
        self._serialized_entity_cache = OrderedDict()

    @property
    def client(self) -> SynchronousFlyteClient:
        """Return a SynchronousFlyteClient for additional operations."""
        return self._client

    @property
    def default_project(self) -> str:
        """Default project to use when fetching or executing flyte entities."""
        return self._default_project

    @property
    def default_domain(self) -> str:
        """Default project to use when fetching or executing flyte entities."""
        return self._default_domain

    @property
    def image_config(self) -> ImageConfig:
        """Image config."""
        return self._image_config

    @property
    def file_access(self) -> FileAccessProvider:
        """File access provider to use for offloading non-literal inputs/outputs."""
        return self._file_access

    @property
    def auth_role(self):
        """Auth role config."""
        return self._auth_role

    @property
    def notifications(self):
        """Notification config."""
        return self._notifications

    @property
    def labels(self):
        """Label config."""
        return self._labels

    @property
    def annotations(self):
        """Annotation config."""
        return self._annotations

    @property
    def raw_output_data_config(self):
        """Location for offloaded data, e.g. in S3"""
        return self._raw_output_data_config

    @property
    def version(self) -> str:
        """Get a randomly generated version string."""
        return uuid.uuid4().hex[:30] + str(int(time.time()))

    def remote_context(self):
        """Context manager with remote-specific configuration."""
        return FlyteContextManager.with_context(
            FlyteContextManager.current_context().with_file_access(self.file_access)
        )

    def with_overrides(
        self,
        default_project: str = None,
        default_domain: str = None,
        flyte_admin_url: str = None,
        insecure: bool = None,
        file_access: typing.Optional[FileAccessProvider] = None,
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
            new_remote._default_project = default_project
        if default_domain:
            new_remote._default_domain = default_domain
        if flyte_admin_url:
            new_remote._flyte_admin_url = flyte_admin_url
            new_remote._client = SynchronousFlyteClient(flyte_admin_url, self._insecure)
        if insecure:
            new_remote._insecure = insecure
            new_remote._client = SynchronousFlyteClient(self._flyte_admin_url, insecure)
        if file_access:
            new_remote._file_access = file_access
        if auth_role:
            new_remote._auth_role = auth_role
        if notifications:
            new_remote._notifications = notifications
        if labels:
            new_remote._labels = labels
        if annotations:
            new_remote._annotations = annotations
        if image_config:
            new_remote._image_config = image_config
        if raw_output_data_config:
            new_remote._raw_output_data_config = raw_output_data_config
        return new_remote

    def fetch_task(self, project: str = None, domain: str = None, name: str = None, version: str = None) -> FlyteTask:
        """Fetch a task entity from flyte admin.

        :param project: fetch entity from this project. If None, uses the default_project attribute.
        :param domain: fetch entity from this domain. If None, uses the default_domain attribute.
        :param name: fetch entity with matching name.
        :param version: fetch entity with matching version. If None, gets the latest version of the entity.
        :returns: :class:`~flytekit.remote.tasks.task.FlyteTask`

        :raises: FlyteAssertion if name is None
        """
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
        """Fetch a workflow entity from flyte admin.

        :param project: fetch entity from this project. If None, uses the default_project attribute.
        :param domain: fetch entity from this domain. If None, uses the default_domain attribute.
        :param name: fetch entity with matching name.
        :param version: fetch entity with matching version. If None, gets the latest version of the entity.
        :returns: :class:`~flytekit.remote.workflow.FlyteWorkflow`

        :raises: FlyteAssertion if name is None
        """
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
        """Fetch a launchplan entity from flyte admin.

        :param project: fetch entity from this project. If None, uses the default_project attribute.
        :param domain: fetch entity from this domain. If None, uses the default_domain attribute.
        :param name: fetch entity with matching name.
        :param version: fetch entity with matching version. If None, gets the latest version of the entity.
        :returns: :class:`~flytekit.remote.launch_plan.FlyteLaunchPlan`

        :raises: FlyteAssertion if name is None
        """
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
        """Fetch a workflow execution entity from flyte admin.

        :param project: fetch entity from this project. If None, uses the default_project attribute.
        :param domain: fetch entity from this domain. If None, uses the default_domain attribute.
        :param name: fetch entity with matching name.
        :returns: :class:`~flytekit.remote.workflow_execution.FlyteWorkflowExecution`

        :raises: FlyteAssertion if name is None
        """
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
        """Serialize an entity for registration."""
        # TODO: Revisit cache
        return get_serializable(
            self._serialized_entity_cache,
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

    @singledispatchmethod
    def register(
        self,
        entity: typing.Union[PythonTask, WorkflowBase, LaunchPlan],
        project: str = None,
        domain: str = None,
        name: str = None,
        version: str = None,
    ) -> typing.Union[FlyteTask, FlyteWorkflow, FlyteLaunchPlan]:
        """Register an entity to flyte admin.

        :param entity: entity to register.
        :param project: register entity into this project. If None, uses ``default_project`` attribute
        :param domain: register entity into this domain. If None, uses ``default_domain`` attribute
        :param name: register entity with this name. If None, uses ``entity.name``
        :param version: register entity with this version. If None, uses auto-generated version.
        """
        raise NotImplementedError(f"entity type {type(entity)} not recognized for registration")

    @register.register
    def _(
        self, entity: PythonTask, project: str = None, domain: str = None, name: str = None, version: str = None
    ) -> FlyteTask:
        """Register an @task-decorated function or TaskTemplate task to flyte admin."""
        resolved_identifiers = asdict(self._resolve_identifier_kwargs(entity, project, domain, name, version))
        self.client.create_task(
            Identifier(ResourceType.TASK, **resolved_identifiers),
            task_spec=self._serialize(entity, **resolved_identifiers),
        )
        return self.fetch_task(**resolved_identifiers)

    @register.register
    def _(
        self, entity: WorkflowBase, project: str = None, domain: str = None, name: str = None, version: str = None
    ) -> FlyteWorkflow:
        """Register an @workflow-decorated function to flyte admin."""
        resolved_identifiers = asdict(self._resolve_identifier_kwargs(entity, project, domain, name, version))
        self.client.create_workflow(
            Identifier(ResourceType.WORKFLOW, **resolved_identifiers),
            workflow_spec=self._serialize(entity, **resolved_identifiers),
        )
        return self.fetch_workflow(**resolved_identifiers)

    @register.register
    def _(
        self, entity: LaunchPlan, project: str = None, domain: str = None, name: str = None, version: str = None
    ) -> FlyteLaunchPlan:
        """Register a LaunchPlan object to flyte admin."""
        # See _get_patch_launch_plan_fn for what we need to patch. These are the elements of a launch plan
        # that are not set at serialization time and are filled in either by flyte-cli register files or flytectl.
        resolved_identifiers = asdict(self._resolve_identifier_kwargs(entity, project, domain, name, version))
        serialized_lp: launch_plan_models.LaunchPlan = self._serialize(entity, **resolved_identifiers)
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
            Identifier(ResourceType.LAUNCH_PLAN, **resolved_identifiers),
            launch_plan_spec=serialized_lp.spec,
        )
        return self.fetch_launch_plan(**resolved_identifiers)

    ####################
    # Execute Entities #
    ####################

    def _resolve_identifier_kwargs(
        self,
        entity,
        project: typing.Optional[str],
        domain: typing.Optional[str],
        name: typing.Optional[str],
        version: typing.Optional[str],
    ) -> ResolvedIdentifiers:
        """
        Resolves the identifier attributes based on user input, falling back on the default project/domain and
        auto-generated version, and ultimately the entity project/domain if entity is a remote flyte entity.
        """
        error_msg = (
            "entity {entity} of type {entity_type} is not associated with a {arg_name}. Please specify the {arg_name} "
            "argument when invoking the FlyteRemote.execute method or a default_{arg_name} value when initializig the "
            "FlyteRemote object."
        )

        if project:
            resolved_project, msg_project = project, "execute-method"
        elif self.default_project:
            resolved_project, msg_project = self.default_project, "remote"
        elif hasattr(entity, "id"):
            resolved_project, msg_project = entity.id.project, "entity"
        else:
            raise TypeError(error_msg.format(entity=entity, entity_type=type(entity), arg_name="project"))

        if domain:
            resolved_domain, msg_domain = domain, "execute-method"
        elif self.default_domain:
            resolved_domain, msg_domain = self.default_domain, "remote"
        elif hasattr(entity, "id"):
            resolved_domain, msg_domain = entity.id.domain, "entity"
        else:
            raise TypeError(error_msg.format(entity=entity, entity_type=type(entity), arg_name="domain"))

        remote_logger.debug(
            f"Using {msg_project}-supplied value for project and {msg_domain}-supplied value for domain."
        )

        return ResolvedIdentifiers(
            resolved_project,
            resolved_domain,
            name or entity.name,
            version or self.version,
        )

    def _execute(
        self,
        flyte_id: Identifier,
        inputs: typing.Dict[str, typing.Any],
        project: str,
        domain: str,
        execution_name: typing.Optional[str] = None,
        wait: bool = False,
    ) -> FlyteWorkflowExecution:
        """Common method for execution across all entities.

        :param flyte_id: entity identifier
        :param inputs: dictionary mapping argument names to values
        :param project: project on which to execute the entity referenced by flyte_id
        :param domain: domain on which to execute the entity referenced by flyte_id
        :param execution_name: name of the execution
        :param wait: if True, waits for execution to complete
        :returns: :class:`~flytekit.remote.workflow_execution.FlyteWorkflowExecution`
        """
        execution_name = execution_name or "f" + uuid.uuid4().hex[:19]
        disable_all = self.notifications == []
        if disable_all:
            notifications = None
        else:
            notifications = NotificationList(self.notifications or [])
            disable_all = None

        with self.remote_context() as ctx:
            literal_inputs = TypeEngine.dict_to_literal_map(ctx, inputs)
        try:
            # TODO: re-consider how this works. Currently, this will only execute the flyte entity referenced by
            # flyte_id in the same project and domain. However, it is possible to execute it in a different project
            # and domain, which is specified in the first two arguments of client.create_execution. This is useful
            # in the case that I want to use a flyte entity from e.g. project "A" but actually execute the entity on a
            # different project "B". For now, this method doesn't support this use case.
            exec_id = self.client.create_execution(
                project,
                domain,
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
        entity: typing.Union[FlyteTask, FlyteLaunchPlan, FlyteWorkflow, PythonTask, WorkflowBase, LaunchPlan],
        inputs: typing.Dict[str, typing.Any],
        project: str = None,
        domain: str = None,
        name: str = None,
        version: str = None,
        execution_name: str = None,
        wait: bool = False,
    ) -> FlyteWorkflowExecution:
        """Execute a task, workflow, or launchplan.

        This method supports:
        - ``Flyte{Task, Workflow, LaunchPlan}`` remote module objects.
        - ``@task``-decorated functions and ``TaskTemplate`` tasks.
        - ``@workflow``-decorated functions.
        - ``LaunchPlan`` objects.

        :param entity: entity to execute
        :param inputs: dictionary mapping argument names to values
        :param project: execute entity in this project. If entity doesn't exist in the project, register the entity
            first before executing.
        :param domain: execute entity in this domain. If entity doesn't exist in the domain, register the entity
            first before executing.
        :param name: execute entity using this name. If not None, use this value instead of ``entity.name``
        :param version: execute entity using this version. If None, uses auto-generated value.
        :param execution_name: name of the execution. If None, uses auto-generated value.
        :param wait: if True, waits for execution to complete

        .. note:

            The ``name`` and ``version`` arguments do not apply to ``FlyteTask``, ``FlyteLaunchPlan``, and
            ``FlyteWorkflow`` entity inputs. These values are determined by referencing the entity identifier values.
        """
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
        execution_name: str = None,
        wait: bool = False,
    ) -> FlyteWorkflowExecution:
        """Execute a FlyteTask, or FlyteLaunchplan.

        NOTE: the name and version arguments are currently not used and only there consistency in the function signature
        """
        if name or version:
            remote_logger.warning(f"The 'name' and 'version' arguments are ignored for entities of type {type(entity)}")
        resolved_identifiers = self._resolve_identifier_kwargs(
            entity, project, domain, entity.id.name, entity.id.version
        )
        return self._execute(
            entity.id,
            inputs,
            project=resolved_identifiers.project,
            domain=resolved_identifiers.domain,
            execution_name=execution_name,
            wait=wait,
        )

    @execute.register
    def _(
        self,
        entity: FlyteWorkflow,
        inputs: typing.Dict[str, typing.Any],
        project: str = None,
        domain: str = None,
        name: str = None,
        version: str = None,
        execution_name: str = None,
        wait: bool = False,
    ) -> FlyteWorkflowExecution:
        """Execute a FlyteWorkflow.

        NOTE: the name and version arguments are currently not used and only there consistency in the function signature
        """
        if name or version:
            remote_logger.warning(f"The 'name' and 'version' arguments are ignored for entities of type {type(entity)}")
        resolved_identifiers = self._resolve_identifier_kwargs(
            entity, project, domain, entity.id.name, entity.id.version
        )
        return self.execute(
            self.fetch_launch_plan(entity.id.project, entity.id.domain, entity.id.name, entity.id.version),
            inputs,
            project=resolved_identifiers.project,
            domain=resolved_identifiers.domain,
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
        wait: bool = False,
    ) -> FlyteWorkflowExecution:
        """Execute an @task-decorated function or TaskTemplate task."""
        resolved_identifiers = self._resolve_identifier_kwargs(entity, project, domain, name, version)
        resolved_identifiers_dict = asdict(resolved_identifiers)
        try:
            flyte_task: FlyteTask = self.fetch_task(**resolved_identifiers_dict)
        except Exception:
            flyte_task: FlyteTask = self.register(entity, **resolved_identifiers_dict)
        return self.execute(
            flyte_task,
            inputs,
            project=resolved_identifiers.project,
            domain=resolved_identifiers.domain,
            execution_name=execution_name,
            wait=wait,
        )

    @execute.register
    def _(
        self,
        entity: WorkflowBase,
        inputs: typing.Dict[str, typing.Any],
        project: str = None,
        domain: str = None,
        name: str = None,
        version: str = None,
        execution_name: str = None,
        wait: bool = False,
    ) -> FlyteWorkflowExecution:
        """Execute an @workflow-decorated function."""
        resolved_identifiers = self._resolve_identifier_kwargs(entity, project, domain, name, version)
        resolved_identifiers_dict = asdict(resolved_identifiers)
        try:
            flyte_workflow: FlyteWorkflow = self.fetch_workflow(**resolved_identifiers_dict)
        except Exception:
            flyte_workflow: FlyteWorkflow = self.register(entity, **resolved_identifiers_dict)
        return self.execute(
            flyte_workflow,
            inputs,
            project=resolved_identifiers.project,
            domain=resolved_identifiers.domain,
            execution_name=execution_name,
            wait=wait,
        )

    @execute.register
    def _(
        self,
        entity: LaunchPlan,
        inputs: typing.Dict[str, typing.Any],
        project: str = None,
        domain: str = None,
        name: str = None,
        version: str = None,
        execution_name: str = None,
        wait: bool = False,
    ) -> FlyteWorkflowExecution:
        """Execute a LaunchPlan object."""
        resolved_identifiers = self._resolve_identifier_kwargs(entity, project, domain, name, version)
        resolved_identifiers_dict = asdict(resolved_identifiers)
        try:
            flyte_launchplan: FlyteLaunchPlan = self.fetch_launch_plan(**resolved_identifiers_dict)
        except Exception:
            flyte_launchplan: FlyteLaunchPlan = self.register(entity, **resolved_identifiers_dict)
        return self.execute(
            flyte_launchplan,
            inputs,
            project=resolved_identifiers.project,
            domain=resolved_identifiers.domain,
            execution_name=execution_name,
            wait=wait,
        )

    ###################################
    # Wait for Executions to Complete #
    ###################################

    def wait(
        self,
        execution: typing.Union[FlyteWorkflowExecution, FlyteNodeExecution, FlyteTaskExecution],
        timeout: typing.Optional[timedelta] = None,
        poll_interval: typing.Optional[timedelta] = None,
    ):
        """Wait for an execution to finish.

        :param execution: execution object to wait on
        :param timeout: maximum amount of time to wait
        :param poll_interval: sync workflow execution at this interval
        """
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
    def sync(
        self,
        execution: typing.Union[FlyteWorkflowExecution, FlyteNodeExecution, FlyteTaskExecution],
        entity_definition: typing.Union[FlyteWorkflow, FlyteTask] = None,
    ):
        """Sync a flyte execution object with its corresponding remote state.

        This method syncs the inputs and outputs of the execution object and all of its child node executions.

        :param execution: workflow execution to sync.
        :param entity_definition: optional, reference entity definition which adds more context to this execution entity
        """
        raise NotImplementedError(f"Execution type {type(execution)} cannot be synced.")

    @sync.register
    def _(
        self, execution: FlyteWorkflowExecution, entity_definition: typing.Union[FlyteWorkflow, FlyteTask] = None
    ) -> FlyteWorkflowExecution:

        """Sync a FlyteWorkflowExecution object with its corresponding remote state."""
        if entity_definition is not None:
            raise ValueError("Entity definition arguments aren't supported when syncing workflow executions")
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
            node.id.node_id: self.sync(FlyteNodeExecution.promote_from_model(node), flyte_entity)
            for node in iterate_node_executions(self.client, execution.id)
        }
        return self._assign_inputs_and_outputs(synced_execution, execution_data, flyte_entity.interface)

    @sync.register
    def _(
        self, execution: FlyteNodeExecution, entity_definition: typing.Union[FlyteWorkflow, FlyteTask] = None
    ) -> FlyteNodeExecution:
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
                self.sync(FlyteNodeExecution.promote_from_model(node), entity_definition)
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
        synced_execution._interface = self._get_node_execution_interface(synced_execution, entity_definition)
        return self._assign_inputs_and_outputs(
            synced_execution,
            self.client.get_node_execution_data(execution.id),
            synced_execution.interface,
        )

    @sync.register
    def _(
        self, execution: FlyteTaskExecution, entity_definition: typing.Union[FlyteWorkflow, FlyteTask] = None
    ) -> FlyteTaskExecution:
        """Sync a FlyteTaskExecution object with its corresponding remote state."""
        if entity_definition is not None:
            raise ValueError("Entity definition arguments aren't supported when syncing task executions")
        synced_execution = deepcopy(execution)

        # sync closure and inputs/outputs
        synced_execution._closure = self.client.get_task_execution(synced_execution.id).closure
        execution_data = self.client.get_task_execution_data(synced_execution.id)
        task_id = execution.id.task_id
        task = self.fetch_task(task_id.project, task_id.domain, task_id.name, task_id.version)
        return self._assign_inputs_and_outputs(synced_execution, execution_data, task.interface)

    #############################
    # Terminate Execution State #
    #############################

    def terminate(self, execution: FlyteWorkflowExecution, cause: str):
        """Terminate a workflow execution.

        :param execution: workflow execution to terminate
        :param cause: reason for termination
        """
        self.client.terminate_execution(execution.id, cause)

    ##################
    # Helper Methods #
    ##################

    def _assign_inputs_and_outputs(self, execution, execution_data, interface):
        """Helper for assigning synced inputs and outputs to an execution object."""
        with self.remote_context() as ctx:
            execution._inputs = TypeEngine.literal_map_to_kwargs(
                ctx=ctx,
                lm=self._get_input_literal_map(execution_data),
                python_types=TypeEngine.guess_python_types(interface.inputs),
            )
            if execution.is_complete and not execution.error:
                execution._outputs = TypeEngine.literal_map_to_kwargs(
                    ctx=ctx,
                    lm=self._get_output_literal_map(execution_data),
                    python_types=TypeEngine.guess_python_types(interface.outputs),
                )
        return execution

    def _get_input_literal_map(self, execution_data: ExecutionDataResponse) -> literal_models.LiteralMap:
        # Inputs are returned inline unless they are too big, in which case a url blob pointing to them is returned.
        if bool(execution_data.full_inputs.literals):
            return execution_data.full_inputs
        elif execution_data.inputs.bytes > 0:
            with self.remote_context() as ctx:
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
            with self.remote_context() as ctx:
                tmp_name = os.path.join(ctx.file_access.local_sandbox_dir, "outputs.pb")
                ctx.file_access.get_data(execution_data.outputs.url, tmp_name)
                return literal_models.LiteralMap.from_flyte_idl(
                    common_utils.load_proto_from_file(literals_pb2.LiteralMap, tmp_name)
                )
        return literal_models.LiteralMap({})

    def _get_node_execution_interface(
        self, node_execution: FlyteNodeExecution, entity_definition: typing.Union[FlyteWorkflow, FlyteTask]
    ) -> TypedInterface:
        """Return the interface of the task or subworkflow associated with this node execution."""
        if isinstance(entity_definition, FlyteTask):
            # A single task execution consists of a Flyte workflow with single node whose interface matches that of
            # the underlying task
            return entity_definition.interface

        for node in entity_definition.flyte_nodes:
            if node.id == node_execution.id.node_id:
                if node.task_node is not None:
                    return node.task_node.flyte_task.interface
                elif node.workflow_node is not None and node.workflow_node.sub_workflow_ref is not None:
                    # Fetch the workflow and use its interface
                    sub_workflow_ref = node.workflow_node.sub_workflow_ref
                    workflow = self.fetch_workflow(
                        sub_workflow_ref.project,
                        sub_workflow_ref.domain,
                        sub_workflow_ref.name,
                        sub_workflow_ref.version,
                    )
                    return workflow.interface
                elif node.workflow_node is not None and node.workflow_node.launchplan_ref is not None:
                    # Fetch the launch plan this node launched, and from there fetch the referenced workflow and use its
                    # interface.
                    lp_ref = node.workflow_node.launchplan_ref
                    launch_plan = self.fetch_launch_plan(lp_ref.project, lp_ref.domain, lp_ref.name, lp_ref.version)
                    workflow = self.fetch_workflow(
                        launch_plan.workflow_id.project,
                        launch_plan.workflow_id.domain,
                        launch_plan.workflow_id.name,
                        launch_plan.workflow_id.version,
                    )
                    return workflow.interface

        # dynamically generated nodes won't have a corresponding node in the compiled workflow closure.
        # in that case, we fetch the interface from the underlying task execution they ran
        if len(node_execution.task_executions) > 0:
            # if not a parent node, assume a task execution node
            task_id = node_execution.task_executions[0].id.task_id
            task = self.fetch_task(task_id.project, task_id.domain, task_id.name, task_id.version)
            return task.interface

        remote_logger.info("failed to find node interface from entity definition closure")
