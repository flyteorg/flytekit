import abc as _abc
import logging as _logging

import six as _six
from sortedcontainers import SortedDict as _SortedDict

from flytekit.common import sdk_bases as _sdk_bases, component_nodes as _component_nodes, constants as _constants
from flytekit.common.exceptions import user as _user_exceptions, system as _system_exceptions, \
    scopes as _exception_scopes
from flytekit.common.mixins import hash as _hash_mixin
from flytekit.common.types import helpers as _type_helpers
from flytekit.common.utils import _dnsify
from flytekit.models import common as _common_models, types as _type_models
from flytekit.models.core import workflow as _workflow_model


class SdkNode(_hash_mixin.HashOnReferenceMixin, _workflow_model.Node, metaclass=_sdk_bases.ExtendedSdkType):
    def __init__(
        self,
        id,
        upstream_nodes,
        bindings,
        metadata,
        sdk_task=None,
        sdk_workflow=None,
        sdk_launch_plan=None,
        sdk_branch=None,
    ):
        """
        :param Text id: A workflow-level unique identifier that identifies this node in the workflow. "inputs" and
            "outputs" are reserved node ids that cannot be used by other nodes.
        :param flytekit.models.core.workflow.NodeMetadata metadata: Extra metadata about the node.
        :param list[flytekit.models.literals.Binding] bindings: Specifies how to bind the underlying
            interface's inputs.  All required inputs specified in the underlying interface must be fulfilled.
        :param list[SdkNode] upstream_nodes: Specifies execution dependencies for this node ensuring it will
            only get scheduled to run after all its upstream nodes have completed. This node will have
            an implicit dependency on any node that appears in inputs field.
        :param flytekit.common.platform.sdk_task.SdkTask sdk_task: The task to execute in this
            node.
        :param flytekit.platform.sdk_workflow.SdkWorkflow sdk_workflow: The workflow to execute in this node.
        :param flytekit.platform.sdk_launch_plan.SdkLaunchPlan sdk_launch_plan: The launch plan to execute in this
        node.
        :param TODO sdk_branch: TODO
        """
        non_none_entities = [
            entity for entity in [sdk_workflow, sdk_branch, sdk_launch_plan, sdk_task] if entity is not None
        ]
        if len(non_none_entities) != 1:
            raise _user_exceptions.FlyteAssertion(
                "An SDK node must have one underlying entity specified at once.  Received the following "
                "entities: {}".format(non_none_entities)
            )

        workflow_node = None
        if sdk_workflow is not None:
            workflow_node = _component_nodes.SdkWorkflowNode(sdk_workflow=sdk_workflow)
        elif sdk_launch_plan is not None:
            workflow_node = _component_nodes.SdkWorkflowNode(sdk_launch_plan=sdk_launch_plan)

        # TODO: this calls the constructor which means it will set all the upstream node ids to None if at the time of
        #       this instantiation, the upstream nodes have not had their nodes assigned yet.
        super(SdkNode, self).__init__(
            id=_dnsify(id) if id else None,
            metadata=metadata,
            inputs=bindings,
            upstream_node_ids=[n.id for n in upstream_nodes],
            output_aliases=[],  # TODO: Are aliases a thing in SDK nodes
            task_node=_component_nodes.SdkTaskNode(sdk_task) if sdk_task else None,
            workflow_node=workflow_node,
            branch_node=sdk_branch,
        )
        self._upstream = upstream_nodes
        self._executable_sdk_object = sdk_task or sdk_workflow or sdk_launch_plan
        if not sdk_branch:
            self._outputs = OutputParameterMapper(self._executable_sdk_object.interface.outputs, self)
        else:
            self._outputs = None

    @property
    def executable_sdk_object(self):
        return self._executable_sdk_object

    @classmethod
    def promote_from_model(cls, model, sub_workflows, tasks):
        """
        :param flytekit.models.core.workflow.Node model:
        :param dict[flytekit.models.core.identifier.Identifier, flytekit.models.core.workflow.WorkflowTemplate]
            sub_workflows:
        :param dict[flytekit.models.core.identifier.Identifier, flytekit.models.task.TaskTemplate] tasks: If specified,
            these task templates will be passed to the SdkTaskNode promote_from_model call, and used
            instead of fetching from Admin.
        :rtype: SdkNode
        """
        id = model.id
        # This should never be called
        if id == _constants.START_NODE_ID or id == _constants.END_NODE_ID:
            _logging.warning("Should not call promote from model on a start node or end node {}".format(model))
            return None

        sdk_task_node, sdk_workflow_node = None, None
        if model.task_node is not None:
            sdk_task_node = _component_nodes.SdkTaskNode.promote_from_model(model.task_node, tasks)
        elif model.workflow_node is not None:
            sdk_workflow_node = _component_nodes.SdkWorkflowNode.promote_from_model(
                model.workflow_node, sub_workflows, tasks
            )
        else:
            raise _system_exceptions.FlyteSystemException("Bad Node model, neither task nor workflow detected")

        # When WorkflowTemplate models (containing node models) are returned by Admin, they've been compiled with a
        # start node.  In order to make the promoted SdkWorkflow look the same, we strip the start-node text back out.
        for i in model.inputs:
            if i.binding.promise is not None and i.binding.promise.node_id == _constants.START_NODE_ID:
                i.binding.promise._node_id = _constants.GLOBAL_INPUT_NODE_ID

        if sdk_task_node is not None:
            return cls(
                id=id,
                upstream_nodes=[],  # set downstream, model doesn't contain this information
                bindings=model.inputs,
                metadata=model.metadata,
                sdk_task=sdk_task_node.sdk_task,
            )
        elif sdk_workflow_node is not None:
            if sdk_workflow_node.sdk_workflow is not None:
                return cls(
                    id=id,
                    upstream_nodes=[],  # set downstream, model doesn't contain this information
                    bindings=model.inputs,
                    metadata=model.metadata,
                    sdk_workflow=sdk_workflow_node.sdk_workflow,
                )
            elif sdk_workflow_node.sdk_launch_plan is not None:
                return cls(
                    id=id,
                    upstream_nodes=[],  # set downstream, model doesn't contain this information
                    bindings=model.inputs,
                    metadata=model.metadata,
                    sdk_launch_plan=sdk_workflow_node.sdk_launch_plan,
                )
            else:
                raise _system_exceptions.FlyteSystemException(
                    "Bad SdkWorkflowNode model, both lp and workflow are None"
                )
        else:
            raise _system_exceptions.FlyteSystemException("Bad SdkNode model, both task and workflow nodes are empty")

    @property
    def upstream_nodes(self):
        """
        :rtype: list[SdkNode]
        """
        return self._upstream

    @property
    def upstream_node_ids(self):
        """
        :rtype: list[Text]
        """
        return [n.id for n in sorted(self.upstream_nodes, key=lambda x: x.id)]

    @property
    def outputs(self):
        """
        :rtype: dict[Text, flytekit.platform.sdk_node.NodeOutput]
        """
        return self._outputs

    def assign_id_and_return(self, id):
        """
        :param Text id:
        :rtype: None
        """
        if self.id:
            raise _user_exceptions.FlyteAssertion(
                "Error assigning ID: {} because {} is already assigned.  Has this node been assigned to another "
                "workflow already?".format(id, self)
            )
        self._id = _dnsify(id) if id else None
        self._metadata._name = id
        return self

    def with_overrides(self, *args, **kwargs):
        # TODO: Implement overrides
        raise NotImplementedError("Overrides are not supported in Flyte yet.")

    @_exception_scopes.system_entry_point
    def __lshift__(self, other):
        """
        Add a node upstream of this node without necessarily mapping outputs -> inputs.
        :param Node other: node to place upstream
        """
        if hash(other) not in set(hash(n) for n in self.upstream_nodes):
            self._upstream.append(other)
        return other

    @_exception_scopes.system_entry_point
    def __rshift__(self, other):
        """
        Add a node downstream of this node without necessarily mapping outputs -> inputs.

        :param Node other: node to place downstream
        """
        if hash(self) not in set(hash(n) for n in other.upstream_nodes):
            other.upstream_nodes.append(self)
        return other

    def __repr__(self):
        """
        :rtype: Text
        """
        return "Node(ID: {} Executable: {})".format(self.id, self._executable_sdk_object)


class ParameterMapper(_SortedDict, metaclass=_common_models.FlyteABCMeta):
    """
    This abstract class provides functionality to reference specific inputs and outputs for a task instance. This
    allows for syntax such as:

        my_task_instance.inputs.my_input

    And is especially useful for linking tasks together via outputs -> inputs in workflow definitions:

        my_second_task_instance(input=my_task_instances.outputs.my_output)

    Attributes:
        Dynamically discovered.  Only the keys for inputs/outputs can be referenced.

    Example:

    .. code-block:: python

        @inputs(a=Types.Integer)
        @outputs(b=Types.String)
        @python_task(version='1')
        def my_task(wf_params, a, b):
            pass

        input_link = my_task.inputs.a # Success!
        output_link = my_tasks.outputs.b # Success!

        input_link = my_task.inputs.c # Attribute not found exception!
        output_link = my_task.outputs.d # Attribute not found exception!

    """

    def __init__(self, type_map, node):
        """
        :param dict[Text, flytekit.models.interface.Variable] type_map:
        :param flytekit.platform.sdk_node.SdkNode node:
        """
        super(ParameterMapper, self).__init__()
        for key, var in _six.iteritems(type_map):
            self[key] = self._return_mapping_object(node, _type_helpers.get_sdk_type_from_literal_type(var.type), key)
        self._initialized = True

    def __getattr__(self, key):
        if key == "iteritems" and hasattr(super(ParameterMapper, self), "items"):
            return super(ParameterMapper, self).items
        if hasattr(super(ParameterMapper, self), key):
            return getattr(super(ParameterMapper, self), key)
        if key not in self:
            raise _user_exceptions.FlyteAssertion("{} doesn't exist.".format(key))
        return self[key]

    def __setattr__(self, key, value):
        if "_initialized" in self.__dict__:
            raise _user_exceptions.FlyteAssertion("Parameters are immutable.")
        else:
            super(ParameterMapper, self).__setattr__(key, value)

    @_abc.abstractmethod
    def _return_mapping_object(self, sdk_node, sdk_type, name):
        """
        :param flytekit.common.nodes.Node sdk_node:
        :param flytekit.common.types.FlyteSdkType sdk_type:
        :param Text name:
        """
        pass


class OutputParameterMapper(ParameterMapper):
    """
    This subclass of ParameterMapper is used to represent outputs for a given node.
    """

    def _return_mapping_object(self, sdk_node, sdk_type, name):
        """
        :param flytekit.common.nodes.Node sdk_node:
        :param flytekit.common.types.FlyteSdkType sdk_type:
        :param Text name:
        """
        return NodeOutput(sdk_node, sdk_type, name)


class NodeOutput(_type_models.OutputReference, metaclass=_sdk_bases.ExtendedSdkType):
    def __init__(self, sdk_node, sdk_type, var):
        """
        :param sdk_node:
        :param sdk_type: deprecated in mypy flytekit.
        :param var:
        """
        self._node = sdk_node
        self._type = sdk_type
        super(NodeOutput, self).__init__(self._node.id, var)

    @property
    def node_id(self):
        """
        Override the underlying node_id property to refer to SdkNode.
        :rtype: Text
        """
        return self.sdk_node.id

    @classmethod
    def promote_from_model(cls, model):
        """
        :param flytekit.models.types.OutputReference model:
        :rtype: NodeOutput
        """
        raise _user_exceptions.FlyteAssertion(
            "A NodeOutput cannot be promoted from a protobuf because it must be "
            "contextualized by an existing SdkNode."
        )

    @property
    def sdk_node(self):
        """
        :rtype: flytekit.platform.sdk_node.SdkNode
        """
        return self._node

    @property
    def sdk_type(self):
        """
        :rtype: flytekit.common.types.base_sdk_types.FlyteSdkType
        """
        return self._type

    def __repr__(self):
        s = f"NodeOutput({self.sdk_node if self.sdk_node.id is not None else None}:{self.var})"
        return s