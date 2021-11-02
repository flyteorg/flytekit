import hashlib as _hashlib
import json as _json
import logging as _logging
import uuid as _uuid

import six as _six
from google.protobuf import json_format as _json_format
from google.protobuf import struct_pb2 as _struct

from flytekit.common import interface as _interfaces
from flytekit.common import nodes as _nodes
from flytekit.common import sdk_bases as _sdk_bases
from flytekit.common import workflow_execution as _workflow_execution
from flytekit.common.core import identifier as _identifier
from flytekit.common.exceptions import scopes as _exception_scopes
from flytekit.common.exceptions import user as _user_exceptions
from flytekit.common.mixins import hash as _hash_mixin
from flytekit.common.mixins import launchable as _launchable_mixin
from flytekit.common.mixins import registerable as _registerable
from flytekit.common.types import helpers as _type_helpers
from flytekit.configuration import auth as _auth_config
from flytekit.configuration import internal as _internal_config
from flytekit.configuration import sdk as _sdk_config
from flytekit.engines.flyte import engine as _flyte_engine
from flytekit.models import common as _common_model
from flytekit.models import execution as _admin_execution_models
from flytekit.models import task as _task_model
from flytekit.models.admin import common as _admin_common
from flytekit.models.core import identifier as _identifier_model
from flytekit.models.core import workflow as _workflow_model


class SdkTask(
    _hash_mixin.HashOnReferenceMixin,
    _registerable.RegisterableEntity,
    _launchable_mixin.LaunchableEntity,
    _task_model.TaskTemplate,
    metaclass=_sdk_bases.ExtendedSdkType,
):
    def __init__(
        self, type, metadata, interface, custom, container=None, task_type_version=0, security_context=None, config=None
    ):
        """
        :param Text type: This is used to define additional extensions for use by Propeller or SDK.
        :param TaskMetadata metadata: This contains information needed at runtime to determine behavior such as
            whether or not outputs are discoverable, timeouts, and retries.
        :param flytekit.common.interface.TypedInterface interface: The interface definition for this task.
        :param dict[Text, T] custom: Arbitrary type for use by plugins.
        :param Container container: Provides the necessary entrypoint information for execution.  For instance,
            a Container might be specified with the necessary command line arguments.
        :param int task_type_version: Specific version of this task type used by plugins to potentially modify
            execution behavior or serialization.
        :param _SecurityContext security_context:
        """
        # TODO: Remove the identifier portion and fill in with local values.
        super(SdkTask, self).__init__(
            _identifier.Identifier(
                _identifier_model.ResourceType.TASK,
                _internal_config.PROJECT.get(),
                _internal_config.DOMAIN.get(),
                _uuid.uuid4().hex,
                _internal_config.VERSION.get(),
            ),
            type,
            metadata,
            interface,
            custom,
            container=container,
            task_type_version=task_type_version,
            security_context=security_context,
            config=config,
        )

    @property
    def interface(self):
        """
        :rtype: flytekit.common.interface.TypedInterface
        """
        return super(SdkTask, self).interface

    @property
    def resource_type(self):
        """
        Integer from _identifier.ResourceType enum
        :rtype: int
        """
        return _identifier_model.ResourceType.TASK

    @property
    def entity_type_text(self):
        """
        :rtype: Text
        """
        return "Task"

    @classmethod
    def promote_from_model(cls, base_model):
        """
        :param flytekit.models.task.TaskTemplate base_model:
        :rtype: SdkTask
        """
        t = cls(
            type=base_model.type,
            metadata=base_model.metadata,
            interface=_interfaces.TypedInterface.promote_from_model(base_model.interface),
            custom=base_model.custom,
            container=base_model.container,
            task_type_version=base_model.task_type_version,
        )
        # Override the newly generated name if one exists in the base model
        if not base_model.id.is_empty:
            t._id = _identifier.Identifier.promote_from_model(base_model.id)

        return t

    def assign_custom_and_return(self, custom):
        self._custom = custom
        return self

    def assign_type_and_return(self, new_type):
        self._type = new_type
        return self

    @_exception_scopes.system_entry_point
    def __call__(self, *args, **input_map):
        """
        :param list[T] args: Do not specify.  Kwargs only are supported for this function.
        :param dict[str, T] input_map: Map of inputs.  Can be statically defined or OutputReference links.
        :rtype: flytekit.common.nodes.SdkNode
        """
        if len(args) > 0:
            raise _user_exceptions.FlyteAssertion(
                "When adding a task as a node in a workflow, all inputs must be specified with kwargs only.  We "
                "detected {} positional args.".format(len(args))
            )

        bindings, upstream_nodes = self.interface.create_bindings_for_inputs(input_map)

        # TODO: Remove DEADBEEF
        # One thing to note - this function is not overloaded at the SdkRunnableTask layer, which means 'self' here
        # will sometimes refer to an object that can be executed locally, and other times will refer to something
        # that cannot (ie a pure SdkTask object, fetched from Admin for instance).
        return _nodes.SdkNode(
            id=None,
            metadata=_workflow_model.NodeMetadata(
                "DEADBEEF",
                self.metadata.timeout,
                self.metadata.retries,
                self.metadata.interruptible,
            ),
            bindings=sorted(bindings, key=lambda b: b.var),
            upstream_nodes=upstream_nodes,
            sdk_task=self,
        )

    @_exception_scopes.system_entry_point
    def register(self, project, domain, name, version):
        """
        :param Text project: The project in which to register this task.
        :param Text domain: The domain in which to register this task.
        :param Text name: The name to give this task.
        :param Text version: The version in which to register this task.
        """
        # TODO: Revisit the notion of supplying the project, domain, name, version, as opposed to relying on the
        #       current ID.
        self.validate()
        id_to_register = _identifier.Identifier(_identifier_model.ResourceType.TASK, project, domain, name, version)
        old_id = self.id

        client = _flyte_engine.get_client()
        try:
            self._id = id_to_register
            client.create_task(id_to_register, _task_model.TaskSpec(self))
            self._id = old_id
            self._has_registered = True
            return str(id_to_register)
        except _user_exceptions.FlyteEntityAlreadyExistsException:
            pass
        except Exception:
            self._id = old_id
            raise

    @_exception_scopes.system_entry_point
    def serialize(self):
        """
        :rtype: flyteidl.admin.task_pb2.TaskSpec
        """
        return _task_model.TaskSpec(self).to_flyte_idl()

    @classmethod
    @_exception_scopes.system_entry_point
    def fetch(cls, project, domain, name, version):
        """
        This function uses the engine loader to call create a hydrated task from Admin.
        :param Text project:
        :param Text domain:
        :param Text name:
        :param Text version:
        :rtype: SdkTask
        """
        task_id = _identifier.Identifier(_identifier_model.ResourceType.TASK, project, domain, name, version)
        admin_task = _flyte_engine.get_client().get_task(task_id)

        sdk_task = cls.promote_from_model(admin_task.closure.compiled_task.template)
        sdk_task._id = task_id
        sdk_task._has_registered = True
        return sdk_task

    @classmethod
    @_exception_scopes.system_entry_point
    def fetch_latest(cls, project, domain, name):
        """
        This function uses the engine loader to call create a latest hydrated task from Admin.
        :param Text project:
        :param Text domain:
        :param Text name:
        :rtype: SdkTask
        """
        named_task = _common_model.NamedEntityIdentifier(project, domain, name)
        client = _flyte_engine.get_client()
        task_list, _ = client.list_tasks_paginated(
            named_task,
            limit=1,
            sort_by=_admin_common.Sort("created_at", _admin_common.Sort.Direction.DESCENDING),
        )
        admin_task = task_list[0] if task_list else None

        if not admin_task:
            raise _user_exceptions.FlyteEntityNotExistException("Named task {} not found".format(named_task))
        sdk_task = cls.promote_from_model(admin_task.closure.compiled_task.template)
        sdk_task._id = admin_task.id
        return sdk_task

    @_exception_scopes.system_entry_point
    def validate(self):
        pass

    @_exception_scopes.system_entry_point
    def add_inputs(self, inputs):
        raise _user_exceptions.FlyteUserException("You can not add inputs to this task")

    @_exception_scopes.system_entry_point
    def add_outputs(self, outputs):
        """
        Adds the outputs to this task.  This can be called multiple times, but it will fail if an output with a given
        name is added more than once, a name collides with an input, or if the name doesn't exist as an arg name in
        the wrapped function.
        :param dict[Text, flytekit.models.interface.Variable] outputs: names and variables to add as outputs
            to this task
        """
        self._validate_outputs(outputs)
        self.interface.outputs.update(outputs)

    def _validate_inputs(self, inputs):
        """
        This method should be overridden in sub-classes that intend to do additional checks on inputs.  If validation
        fails, this function should raise an informative exception.
        :param dict[Text, flytekit.models.interface.Variable] inputs:  Input variables to validate
        :raises: flytekit.common.exceptions.user.FlyteValidationException
        """
        for k, v in _six.iteritems(inputs):
            if k in self.interface.inputs:
                raise _user_exceptions.FlyteValidationException(
                    "An input with name '{}' is already defined.  Redefinition is not allowed.".format(k)
                )
            if k in self.interface.outputs:
                raise _user_exceptions.FlyteValidationException(
                    "An output with name '{}' is already defined.  Therefore '{}' can't be defined as an "
                    "input".format(k, v)
                )

    def _validate_outputs(self, outputs):
        """
        This method should be overridden in sub-classes that intend to do additional checks on outputs.  If validation
        fails, this function should raise an informative exception.
        :param dict[Text, flytekit.models.interface.Variable] outputs:  Output variables to validate
        :raises: flytekit.common.exceptions.user.FlyteValidationException
        """
        for k, v in _six.iteritems(outputs):
            if k in self.interface.outputs:
                raise _user_exceptions.FlyteValidationException(
                    "An output with name '{}' is already defined.  Redefinition is not allowed.".format(k)
                )
            if k in self.interface.inputs:
                raise _user_exceptions.FlyteValidationException(
                    "An input with name '{}' is already defined.  Therefore '{}' can't be defined as an "
                    "input".format(k, v)
                )

    def __repr__(self):
        return "Flyte {task_type}: {interface}".format(task_type=self.type, interface=self.interface)

    def _python_std_input_map_to_literal_map(self, inputs):
        """
        :param dict[Text,Any] inputs: A dictionary of Python standard inputs that will be type-checked and compiled
            to a LiteralMap
        :rtype: flytekit.models.literals.LiteralMap
        """
        return _type_helpers.pack_python_std_map_to_literal_map(
            inputs,
            {k: _type_helpers.get_sdk_type_from_literal_type(v.type) for k, v in _six.iteritems(self.interface.inputs)},
        )

    def _produce_deterministic_version(self, version=None):
        """
        :param Text version:
        :return Text:
        """

        if self.container is not None and self.container.data_loading_config is None:
            # Only in the case of raw container tasks (which are the only valid tasks with container definitions that
            # can assign a client-side task version) their data config will be None.
            raise ValueError("Client-side task versions are not supported for {} task type".format(self.type))
        if version is not None:
            return version
        custom = _json_format.Parse(_json.dumps(self.custom, sort_keys=True), _struct.Struct()) if self.custom else None

        # The task body is the entirety of the task template MINUS the identifier. The identifier is omitted because
        # 1) this method is used to compute the version portion of the identifier and
        # 2 ) the SDK will actually generate a unique name on every task instantiation which is not great for
        # the reproducibility this method attempts.
        task_body = (
            self.type,
            self.metadata.to_flyte_idl().SerializeToString(deterministic=True),
            self.interface.to_flyte_idl().SerializeToString(deterministic=True),
            custom,
        )
        return _hashlib.md5(str(task_body).encode("utf-8")).hexdigest()

    @_exception_scopes.system_entry_point
    def register_and_launch(self, project, domain, name, version=None, inputs=None):
        """
        :param Text project: The project in which to register and launch this task.
        :param Text domain: The domain in which to register and launch this task.
        :param Text name: The name to give this task.
        :param Text version: The version in which to register this task
        :param dict[Text, Any] inputs: A dictionary of Python standard inputs that will be type-checked, then compiled
            to a LiteralMap.

        :rtype: flytekit.common.workflow_execution.SdkWorkflowExecution
        """
        self.validate()
        version = self._produce_deterministic_version(version)
        self.register(project, domain, name, version)
        return self.launch(project, domain, inputs=inputs)

    @_exception_scopes.system_entry_point
    def launch_with_literals(
        self,
        project,
        domain,
        literal_inputs,
        name=None,
        notification_overrides=None,
        label_overrides=None,
        annotation_overrides=None,
        auth_role=None,
    ):
        """
        Launches a single task execution and returns the execution identifier.
        :param Text project:
        :param Text domain:
        :param flytekit.models.literals.LiteralMap literal_inputs: Inputs to the execution.
        :param Text name: [Optional] If specified, an execution will be created with this name.  Note: the name must
            be unique within the context of the project and domain.
        :param list[flytekit.common.notifications.Notification] notification_overrides: [Optional] If specified, these
            are the notifications that will be honored for this execution.  An empty list signals to disable all
            notifications.
        :param flytekit.models.common.Labels label_overrides:
        :param flytekit.models.common.Annotations annotation_overrides:
        :param flytekit.models.common.AuthRole auth_role:
        :rtype: flytekit.common.workflow_execution.SdkWorkflowExecution
        """
        disable_all = notification_overrides == []
        if disable_all:
            notification_overrides = None
        else:
            notification_overrides = _admin_execution_models.NotificationList(notification_overrides or [])
            disable_all = None

        # Unlike regular workflow executions, single task executions must always specify an auth role, since there isn't
        # any existing launch plan with a bound auth role to fall back on.
        if auth_role is None:
            assumable_iam_role = _auth_config.ASSUMABLE_IAM_ROLE.get()
            kubernetes_service_account = _auth_config.KUBERNETES_SERVICE_ACCOUNT.get()

            if not (assumable_iam_role or kubernetes_service_account):
                _logging.warning(
                    "Using deprecated `role` from config. "
                    "Please update your config to use `assumable_iam_role` instead"
                )
                assumable_iam_role = _sdk_config.ROLE.get()
            auth_role = _common_model.AuthRole(
                assumable_iam_role=assumable_iam_role,
                kubernetes_service_account=kubernetes_service_account,
            )

        client = _flyte_engine.get_client()
        try:
            # TODO(katrogan): Add handling to register the underlying task if it's not already.
            exec_id = client.create_execution(
                project,
                domain,
                name,
                _admin_execution_models.ExecutionSpec(
                    self.id,
                    _admin_execution_models.ExecutionMetadata(
                        _admin_execution_models.ExecutionMetadata.ExecutionMode.MANUAL,
                        "sdk",  # TODO: get principle
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
        except _user_exceptions.FlyteEntityAlreadyExistsException:
            exec_id = _identifier.WorkflowExecutionIdentifier(project, domain, name)
        execution = client.get_execution(exec_id)
        return _workflow_execution.SdkWorkflowExecution.promote_from_model(execution)
