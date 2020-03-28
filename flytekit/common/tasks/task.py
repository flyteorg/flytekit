from __future__ import absolute_import

import uuid as _uuid

import six as _six

from flytekit.common import interface as _interfaces, nodes as _nodes, sdk_bases as _sdk_bases
from flytekit.common.core import identifier as _identifier
from flytekit.common.exceptions import scopes as _exception_scopes
from flytekit.common.mixins import registerable as _registerable, hash as _hash_mixin
from flytekit.configuration import internal as _internal_config
from flytekit.engines import loader as _engine_loader
from flytekit.models import common as _common_model, task as _task_model
from flytekit.models.core import workflow as _workflow_model, identifier as _identifier_model
from flytekit.common.exceptions import user as _user_exceptions


class SdkTask(
    _six.with_metaclass(
        _sdk_bases.ExtendedSdkType,
        _hash_mixin.HashOnReferenceMixin,
        _task_model.TaskTemplate,
        _registerable.RegisterableEntity,
    )
):

    def __init__(self, type, metadata, interface, custom, container=None):
        """
        :param Text type: This is used to define additional extensions for use by Propeller or SDK.
        :param TaskMetadata metadata: This contains information needed at runtime to determine behavior such as
            whether or not outputs are discoverable, timeouts, and retries.
        :param flytekit.common.interface.TypedInterface interface: The interface definition for this task.
        :param dict[Text, T] custom: Arbitrary type for use by plugins.
        :param Container container: Provides the necessary entrypoint information for execution.  For instance,
            a Container might be specified with the necessary command line arguments.
        """
        super(SdkTask, self).__init__(
            _identifier.Identifier(
                _identifier_model.ResourceType.TASK,
                _internal_config.PROJECT.get(),
                _internal_config.DOMAIN.get(),
                _uuid.uuid4().hex,
                _internal_config.VERSION.get()
            ),
            type,
            metadata,
            interface,
            custom,
            container=container
        )

    @property
    def interface(self):
        """
        :rtype: flytekit.common.interface.TypedInterface
        """
        return super(SdkTask, self).interface

    @property
    def upstream_entities(self):
        """
        Task, workflow, and launch plan that need to be registered in advance of this workflow.
        :rtype: set[T]
        """
        return set()

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
            container=base_model.container
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
        return _nodes.SdkNode(
            id=None,
            metadata=_workflow_model.NodeMetadata("DEADBEEF", self.metadata.timeout, self.metadata.retries, self.metadata.interruptible),
            bindings=sorted(bindings, key=lambda b: b.var),
            upstream_nodes=upstream_nodes,
            sdk_task=self
        )

    @_exception_scopes.system_entry_point
    def register(self, project, domain, name, version):
        """
        :param Text project: The project in which to register this task.
        :param Text domain: The domain in which to register this task.
        :param Text name: The name to give this task.
        :param Text version: The version in which to register this task.
        """
        self.validate()
        id_to_register = _identifier.Identifier(_identifier_model.ResourceType.TASK, project, domain, name, version)
        old_id = self.id
        try:
            self._id = id_to_register
            _engine_loader.get_engine().get_task(self).register(id_to_register)
            return _six.text_type(self.id)
        except:
            self._id = old_id
            raise

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
        admin_task = _engine_loader.get_engine().fetch_task(task_id=task_id)
        sdk_task = cls.promote_from_model(admin_task.closure.compiled_task.template)
        sdk_task._id = task_id
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
        admin_task = _engine_loader.get_engine().fetch_latest_task(named_task)
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
        return "Flyte {task_type}: {interface}".format(
            task_type=self.type,
            interface=self.interface
        )
