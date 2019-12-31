from __future__ import absolute_import

import uuid as _uuid

import six as _six
from six.moves import queue as _queue

from flytekit.common import interface as _interface, nodes as _nodes, sdk_bases as _sdk_bases, \
    launch_plan as _launch_plan, promise as _promise
from flytekit.common.core import identifier as _identifier
from flytekit.common.tasks import task as _task
from flytekit.common.exceptions import scopes as _exception_scopes, user as _user_exceptions
from flytekit.common.mixins import registerable as _registerable, hash as _hash_mixin
from flytekit.common.types import helpers as _type_helpers
from flytekit.common.promise import Input as _Input
from flytekit.configuration import internal as _internal_config
from flytekit.engines import loader as _engine_loader
from flytekit.models import interface as _interface_models, literals as _literal_models, \
    launch_plan as _launch_plan_models
from flytekit.models.core import workflow as _workflow_models, identifier as _identifier_model


class Output(object):

    def __init__(self, name, value, sdk_type=None, binding_data=None, help=None):
        """
        :param Text name:
        :param T value:
        :param U sdk_type: If specified, the value provided must cast to this type.  Normally should be an instance of
            flytekit.common.types.base_sdk_types.FlyteSdkType.  But could also be something like:

            list[flytekit.common.types.base_sdk_types.FlyteSdkType],
            dict[flytekit.common.types.base_sdk_types.FlyteSdkType,flytekit.common.types.base_sdk_types.FlyteSdkType],
            (flytekit.common.types.base_sdk_types.FlyteSdkType, flytekit.common.types.base_sdk_types.FlyteSdkType, ...)
        """
        if sdk_type is None:
            # This syntax didn't work for some reason: sdk_type = sdk_type or Output._infer_type(value)
            sdk_type = Output._infer_type(value)
        sdk_type = _type_helpers.python_std_to_sdk_type(sdk_type)

        if binding_data is None:
            self._binding_data = _interface.BindingData.from_python_std(sdk_type.to_flyte_literal_type(), value)
        else:
            self._binding_data = binding_data
        self._var = _interface_models.Variable(sdk_type.to_flyte_literal_type(), help or '')
        self._name = name

    def rename_and_return_reference(self, new_name):
        self._name = new_name
        return self

    @staticmethod
    def _infer_type(value):
        # TODO: Infer types
        raise NotImplementedError("Currently the SDK cannot infer a workflow output type, so please use the type kwarg "
                                  "when instantiating an output.")

    @property
    def name(self):
        """
        :rtype: Text
        """
        return self._name

    @property
    def binding_data(self):
        """
        :rtype: flytekit.models.literals.BindingData
        """
        return self._binding_data

    @property
    def var(self):
        """
        :rtype: flytekit.models.interface.Variable
        """
        return self._var


class SdkWorkflow(
    _six.with_metaclass(
        _sdk_bases.ExtendedSdkType,
        _hash_mixin.HashOnReferenceMixin,
        _workflow_models.WorkflowTemplate,
        _registerable.RegisterableEntity,
    )
):

    def __init__(self, inputs, outputs, nodes):
        """
        :param list[flytekit.common.promise.Input] inputs:
        :param list[Output] outputs:
        :param list[flytekit.common.nodes.SdkNode] nodes:
        """
        for n in nodes:
            for upstream in n.upstream_nodes:
                if upstream.id is None:
                    raise _user_exceptions.FlyteAssertion(
                        "Some nodes contained in the workflow were not found in the workflow description.  Please "
                        "ensure all nodes are either assigned to attributes within the class or an element in a "
                        "list, dict, or tuple which is stored as an attribute in the class."
                    )

        super(SdkWorkflow, self).__init__(
            id=_identifier.Identifier(
                _identifier_model.ResourceType.WORKFLOW,
                _internal_config.PROJECT.get(),
                _internal_config.DOMAIN.get(),
                _uuid.uuid4().hex,
                _internal_config.VERSION.get()
            ),
            metadata=_workflow_models.WorkflowMetadata(),
            interface=_interface.TypedInterface(
                {v.name: v.var for v in inputs},
                {v.name: v.var for v in outputs}
            ),
            nodes=nodes,
            outputs=[_literal_models.Binding(v.name, v.binding_data) for v in outputs],
        )
        self._user_inputs = inputs
        self._upstream_entities = set(n.executable_sdk_object for n in nodes)

    @property
    def upstream_entities(self):
        """
        Task, workflow, and launch plan that need to be registered in advance of this workflow.
        :rtype: set[T]
        """
        return self._upstream_entities

    @property
    def interface(self):
        """
        :rtype: flytekit.common.interface.TypedInterface
        """
        return super(SdkWorkflow, self).interface

    @property
    def entity_type_text(self):
        """
        :rtype: Text
        """
        return "Workflow"

    @property
    def resource_type(self):
        """
        Integer from _identifier.ResourceType enum
        :rtype: int
        """
        return _identifier_model.ResourceType.WORKFLOW

    @property
    def user_inputs(self):
        """
        :rtype: list[flytekit.common.promise.Input]
        """
        return self._user_inputs

    @classmethod
    @_exception_scopes.system_entry_point
    def fetch(cls, project, domain, name, version=None):
        """
        This function uses the engine loader to call create a hydrated task from Admin.
        :param Text project:
        :param Text domain:
        :param Text name:
        :param Text version:
        :rtype: SdkWorkflow
        """
        version = version or _internal_config.VERSION.get()
        workflow_id = _identifier.Identifier(_identifier_model.ResourceType.WORKFLOW, project, domain, name, version)
        admin_workflow = _engine_loader.get_engine().fetch_workflow(workflow_id)
        sdk_workflow = cls.promote_from_model(admin_workflow.closure.compiled_workflow.primary.template)
        sdk_workflow._id = workflow_id
        return sdk_workflow

    @classmethod
    def promote_from_model(cls, base_model):
        """
        :param flytekit.models.core.workflow.WorkflowTemplate base_model:
        :rtype: SdkWorkflow
        """
        # TODO: To save on task fetches (for workflows with a lot of repeated tasks), we should cache the results of
        #  the fetch, just in-process
        node_map = {
            n.id: _nodes.SdkNode(
                n.id,
                [],
                n.inputs,
                n.metadata,
                sdk_task=_task.SdkTask.fetch(n.task_node.reference_id.project, n.task_node.reference_id.domain,
                                        n.task_node.reference_id.name, n.task_node.reference_id.version),
                sdk_workflow=None,
                sdk_branch=None  # TODO: Hydrate these objects by reference from the engine.
            )
            for n in base_model.nodes
        }

        for v in _six.itervalues(node_map):
            v.upstream_nodes[:] = [node_map[k] for k in v.upstream_node_ids]

        iface = _interface.TypedInterface.promote_from_model(base_model.interface)
        inputs = [_Input(name=k, sdk_type=_type_helpers.get_sdk_type_from_literal_type(v.type), required=True) for k, v
                  in _six.iteritems(iface.inputs)]
        outputs = [Output(name=o.var, value=None, binding_data=o.binding,
                          sdk_type=_type_helpers.get_sdk_type_from_literal_type(iface.outputs[o.var].type)) for o in
                   base_model.outputs]
        return cls(
            inputs=inputs,
            outputs=outputs,
            nodes=list(node_map.values()),
        )

    @_exception_scopes.system_entry_point
    def register(self, project, domain, name, version):
        """
        :param Text project:
        :param Text domain:
        :param Text name:
        :param Text version:
        """
        self.validate()
        id_to_register = _identifier.Identifier(
            _identifier_model.ResourceType.WORKFLOW,
            project,
            domain,
            name,
            version
        )
        old_id = self.id
        try:
            self._id = id_to_register
            _engine_loader.get_engine().get_workflow(self).register(id_to_register)
            return _six.text_type(self.id)
        except:
            self._id = old_id
            raise

    @_exception_scopes.system_entry_point
    def validate(self):
        pass

    @_exception_scopes.system_entry_point
    def create_launch_plan(
            self,
            default_inputs=None,
            fixed_inputs=None,
            schedule=None,
            role=None,
            notifications=None,
            labels=None,
            annotations=None,
            assumable_iam_role=None,
            kubernetes_service_account=None,
            cls=None
    ):
        """
        This method will create a launch plan object that can execute this workflow.
        :param dict[Text,flytekit.common.promise.Input] default_inputs:
        :param dict[Text,T] fixed_inputs:
        :param flytekit.models.schedule.Schedule schedule: A schedule on which to execute this launch plan.
        :param Text role: Deprecated. Use assumable_iam_role instead.
        :param list[flytekit.models.common.Notification] notifications: A list of notifications to enact by default for
        this launch plan.
        :param flytekit.models.common.Labels labels:
        :param flytekit.models.common.Annotations annotations:
        :param cls: This parameter can be used by users to define an extension of a launch plan to instantiate.  The
        class provided should be a subclass of flytekit.common.launch_plan.SdkLaunchPlan.
        :param Text assumable_iam_role: The IAM role to execute the workflow with.
        :param Text kubernetes_service_account: The kubernetes service account to execute the workflow with.
        :rtype: flytekit.common.launch_plan.SdkRunnableLaunchPlan
        """
        # TODO: Actually ensure the parameters conform.
        if role and (assumable_iam_role or kubernetes_service_account):
            raise ValueError("Cannot set both role and auth. Role is deprecated, use auth instead.")
        fixed_inputs = fixed_inputs or {}
        merged_default_inputs = {v.name: v for v in self._user_inputs if v.name not in fixed_inputs}
        merged_default_inputs.update(default_inputs or {})

        if role:
            assumable_iam_role = role
        auth = _launch_plan_models.Auth(assumable_iam_role=assumable_iam_role,
                                        kubernetes_service_account=kubernetes_service_account)

        return (cls or _launch_plan.SdkRunnableLaunchPlan)(
            sdk_workflow=self,
            default_inputs={
                k: user_input.rename_and_return_reference(k)
                for k, user_input in _six.iteritems(merged_default_inputs)
            },
            fixed_inputs=fixed_inputs,
            schedule=schedule,
            notifications=notifications,
            labels=labels,
            annotations=annotations,
            auth=auth,
        )

    @_exception_scopes.system_entry_point
    def __call__(self, *args, **kwargs):
        # TODO: Create a workflow node
        raise NotImplementedError("Embedding a workflow as a node is not supported currently.  Please use launch "
                                  "plans.")


def _assign_indexed_attribute_name(attribute_name, index):
    return "{}[{}]".format(attribute_name, index)


def _discover_workflow_components(workflow_class):
    """
    This task iterates over the attributes of a user-defined class in order to return a list of inputs, outputs and
    nodes.
    :param class workflow_class: User-defined class with task instances as attributes.
    :rtype: (list[flytekit.common.promise.Input], list[Output], list[flytekit.common.nodes.SdkNode])
    """

    inputs = []
    outputs = []
    nodes = []

    to_visit_objs = _queue.Queue()
    top_level_attributes = set()
    for attribute_name in dir(workflow_class):
        to_visit_objs.put((attribute_name, getattr(workflow_class, attribute_name)))
        top_level_attributes.add(attribute_name)

    # For all task instances defined within the workflow, bind them to this specific workflow and hook-up to the
    # engine (when available)
    visited_obj_ids = set()
    while not to_visit_objs.empty():
        attribute_name, current_obj = to_visit_objs.get()

        current_obj_id = id(current_obj)
        if current_obj_id in visited_obj_ids:
            continue
        visited_obj_ids.add(current_obj_id)

        if isinstance(current_obj, _nodes.SdkNode):
            # TODO: If an attribute name is on the form node_name[index], the resulting
            # node name might not be correct.
            nodes.append(current_obj.assign_id_and_return(attribute_name))
        elif isinstance(current_obj, _promise.Input):
            if attribute_name is None or attribute_name not in top_level_attributes:
                raise _user_exceptions.FlyteValueException(
                    attribute_name,
                    "Detected workflow input specified outside of top level."
                )
            inputs.append(current_obj.rename_and_return_reference(attribute_name))
        elif isinstance(current_obj, Output):
            if attribute_name is None or attribute_name not in top_level_attributes:
                raise _user_exceptions.FlyteValueException(
                    attribute_name,
                    "Detected workflow output specified outside of top level."
                )
            outputs.append(current_obj.rename_and_return_reference(attribute_name))
        elif isinstance(current_obj, list) or isinstance(current_obj, set) or isinstance(current_obj, tuple):
            for idx, value in enumerate(current_obj):
                to_visit_objs.put(
                    (_assign_indexed_attribute_name(attribute_name, idx), value))
        elif isinstance(current_obj, dict):
            # Visit dictionary keys.
            for key in current_obj.keys():
                to_visit_objs.put(
                    (_assign_indexed_attribute_name(attribute_name, key), key))
            # Visit dictionary values.
            for key, value in _six.iteritems(current_obj):
                to_visit_objs.put(
                    (_assign_indexed_attribute_name(attribute_name, key), value))
    return inputs, outputs, nodes


def build_sdk_workflow_from_metaclass(metaclass, cls=None):
    """
    :param T metaclass:
    :param cls: This is the class that will be instantiated from the inputs, outputs, and nodes.  This will be used
        by users extending the base Flyte programming model. If set, it must be a subclass of SdkWorkflow.
    :rtype: SdkWorkflow
    """
    inputs, outputs, nodes = _discover_workflow_components(metaclass)

    return (cls or SdkWorkflow)(
        inputs=[i for i in sorted(inputs, key=lambda x: x.name)],
        outputs=[o for o in sorted(outputs, key=lambda x: x.name)],
        nodes=[n for n in sorted(nodes, key=lambda x: x.id)]
    )
