from __future__ import absolute_import

import abc as _abc
import logging as _logging

import six as _six
from sortedcontainers import SortedDict as _SortedDict

from flytekit.common import constants as _constants
from flytekit.common import sdk_bases as _sdk_bases, promise as _promise, component_nodes as _component_nodes
from flytekit.common.exceptions import scopes as _exception_scopes, user as _user_exceptions
from flytekit.common.exceptions import system as _system_exceptions
from flytekit.common.mixins import hash as _hash_mixin, artifact as _artifact_mixin
from flytekit.common.tasks import executions as _task_executions
from flytekit.common.types import helpers as _type_helpers
from flytekit.common.utils import _dnsify
from flytekit.engines import loader as _engine_loader
from flytekit.models import common as _common_models, node_execution as _node_execution_models
from flytekit.models.core import workflow as _workflow_model, execution as _execution_models


class ParameterMapper(_six.with_metaclass(_common_models.FlyteABCMeta, _SortedDict)):
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
        :param SdkNode node:
        """
        super(ParameterMapper, self).__init__()
        for key, var in _six.iteritems(type_map):
            self[key] = self._return_mapping_object(node, _type_helpers.get_sdk_type_from_literal_type(var.type), key)
        self._initialized = True

    def __getattr__(self, key):
        if key == 'iteritems' and hasattr(super(ParameterMapper, self), 'items'):
            return super(ParameterMapper, self).items
        if hasattr(super(ParameterMapper, self), key):
            return getattr(super(ParameterMapper, self), key)
        if key not in self:
            raise _user_exceptions.FlyteAssertion("{} doesn't exist.".format(key))
        return self[key]

    def __setattr__(self, key, value):
        if '_initialized' in self.__dict__:
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
        return _promise.NodeOutput(sdk_node, sdk_type, name)


class SdkNode(_six.with_metaclass(_sdk_bases.ExtendedSdkType, _hash_mixin.HashOnReferenceMixin, _workflow_model.Node)):

    def __init__(
            self,
            id,
            upstream_nodes,
            bindings,
            metadata,
            sdk_task=None,
            sdk_workflow=None,
            sdk_launch_plan=None,
            sdk_branch=None
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
        :param flytekit.common.tasks.task.SdkTask sdk_task: The task to execute in this
            node.
        :param flytekit.common.workflow.SdkWorkflow sdk_workflow: The workflow to execute in this node.
        :param flytekit.common.launch_plan.SdkLaunchPlan sdk_launch_plan: The launch plan to execute in this
        node.
        :param TODO sdk_branch: TODO
        """
        non_none_entities = [
            entity
            for entity in [sdk_workflow, sdk_branch, sdk_launch_plan, sdk_task] if entity is not None
        ]
        if len(non_none_entities) != 1:
            raise _user_exceptions.FlyteAssertion(
                "An SDK node must have one underlying entity specified at once.  Received the following "
                "entities: {}".format(
                    non_none_entities
                )
            )

        workflow_node = None
        if sdk_workflow is not None:
            workflow_node = _component_nodes.SdkWorkflowNode(sdk_workflow=sdk_workflow)
        elif sdk_launch_plan is not None:
            workflow_node = _component_nodes.SdkWorkflowNode(sdk_launch_plan=sdk_launch_plan)

        super(SdkNode, self).__init__(
            id=_dnsify(id) if id else None,
            metadata=metadata,
            inputs=bindings,
            upstream_node_ids=[n.id for n in upstream_nodes],
            output_aliases=[],  # TODO: Are aliases a thing in SDK nodes
            task_node=_component_nodes.SdkTaskNode(sdk_task) if sdk_task else None,
            workflow_node=workflow_node,
            branch_node=sdk_branch.target if sdk_branch else None
        )
        self._upstream = upstream_nodes
        self._executable_sdk_object = sdk_task or sdk_workflow or sdk_branch or sdk_launch_plan
        self._outputs = OutputParameterMapper(self._executable_sdk_object.interface.outputs, self)

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
                model.workflow_node, sub_workflows, tasks)
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
                    "Bad SdkWorkflowNode model, both lp and workflow are None")
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
        :rtype: dict[Text, flytekit.common.promise.NodeOutput]
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


class SdkNodeExecution(
    _six.with_metaclass(
        _sdk_bases.ExtendedSdkType,
        _node_execution_models.NodeExecution,
        _artifact_mixin.ExecutionArtifact
    )
):
    def __init__(self, *args, **kwargs):
        super(SdkNodeExecution, self).__init__(*args, **kwargs)
        self._task_executions = None
        self._workflow_executions = None
        self._inputs = None
        self._outputs = None

    @property
    def task_executions(self):
        """
        Returns the underlying task executions in order of try attempt.
        :rtype: list[flytekit.common.tasks.executions.SdkTaskExecution]
        """
        return self._task_executions or []

    @property
    def workflow_executions(self):
        """
        Returns the underlying workflow executions in order of try attempt.
        :rtype: list[flytekit.common.workflow_execution.SdkWorkflowExecution]
        """
        return self._workflow_executions or []

    @property
    def executions(self):
        """
        Returns a list of generic execution artifacts.
        :rtype: list[flytekit.common.mixins.artifact.ExecutionArtifact]
        """
        return self.task_executions or self.workflow_executions or []

    @property
    def inputs(self):
        """
        Returns the inputs to the execution in the standard Python format as dictated by the type engine.
        :rtype: dict[Text, T]
        """
        if self._inputs is None:
            self._inputs = _type_helpers.unpack_literal_map_to_sdk_python_std(
                _engine_loader.get_engine().get_node_execution(self).get_inputs()
            )
        return self._inputs

    @property
    def outputs(self):
        """
        Returns the outputs to the execution in the standard Python format as dictated by the type engine.  If the
        execution ended in error or the execution is in progress, an exception will be raised.
        :rtype: dict[Text, T]
        """
        if not self.is_complete:
            raise _user_exceptions.FlyteAssertion("Please what until the node execution has completed before "
                                                  "requesting the outputs.")
        if self.error:
            raise _user_exceptions.FlyteAssertion("Outputs could not be found because the execution ended in failure.")

        if self._outputs is None:
            self._outputs = _type_helpers.unpack_literal_map_to_sdk_python_std(
                _engine_loader.get_engine().get_node_execution(self).get_outputs()
            )
        return self._outputs

    @property
    def error(self):
        """
        If execution is in progress, raise an exception.  Otherwise, return None if no error was present upon
        reaching completion.
        :rtype: flytekit.models.core.execution.ExecutionError or None
        """
        if not self.is_complete:
            raise _user_exceptions.FlyteAssertion("Please what until the node execution has completed before "
                                                  "requesting error information.")
        return self.closure.error

    @property
    def is_complete(self):
        """
        Dictates whether or not the execution is complete.
        :rtype: bool
        """
        return self.closure.phase in {
            _execution_models.NodeExecutionPhase.ABORTED,
            _execution_models.NodeExecutionPhase.FAILED,
            _execution_models.NodeExecutionPhase.SKIPPED,
            _execution_models.NodeExecutionPhase.SUCCEEDED,
            _execution_models.NodeExecutionPhase.TIMED_OUT,
        }

    @classmethod
    def promote_from_model(cls, base_model):
        """
        :param _node_execution_models.NodeExecution base_model:
        :rtype: SdkNodeExecution
        """
        return cls(
            closure=base_model.closure,
            id=base_model.id,
            input_uri=base_model.input_uri
        )

    def sync(self):
        """
        Syncs the state of this object with that held by the platform.
        :rtype: None
        """
        if not self.is_complete or self.task_executions is not None:
            ne = _engine_loader.get_engine().get_node_execution(self)
            ne.sync()
            self._task_executions = [
                _task_executions.SdkTaskExecution.promote_from_model(te) for te in ne.get_task_executions()
            ]
            # TODO: Sub-workflows too once implemented
