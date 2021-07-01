import logging as _logging
import os as _os
from typing import Any, Dict, List, Optional

from flyteidl.core import literals_pb2 as _literals_pb2

import flytekit
from flytekit.clients.helpers import iterate_node_executions, iterate_task_executions
from flytekit.common import constants as _constants
from flytekit.common import utils as _common_utils
from flytekit.common.exceptions import system as _system_exceptions
from flytekit.common.exceptions import user as _user_exceptions
from flytekit.common.mixins import artifact as _artifact_mixin
from flytekit.common.mixins import hash as _hash_mixin
from flytekit.common.utils import _dnsify
from flytekit.core.context_manager import FlyteContextManager
from flytekit.core.promise import NodeOutput
from flytekit.core.type_engine import TypeEngine
from flytekit.engines.flyte import engine as _flyte_engine
from flytekit.interfaces.data import data_proxy as _data_proxy
from flytekit.models import literals as _literal_models
from flytekit.models import node_execution as _node_execution_models
from flytekit.models import task as _task_model
from flytekit.models.core import execution as _execution_models
from flytekit.models.core import workflow as _workflow_model
from flytekit.remote import component_nodes as _component_nodes
from flytekit.remote import identifier as _identifier
from flytekit.remote.tasks.executions import FlyteTaskExecution


class FlyteNode(_hash_mixin.HashOnReferenceMixin, _workflow_model.Node):
    def __init__(
        self,
        id,
        upstream_nodes,
        bindings,
        metadata,
        flyte_task: "flytekit.remote.tasks.task.FlyteTask" = None,
        flyte_workflow: "flytekit.remote.workflow.FlyteWorkflow" = None,
        flyte_launch_plan=None,
        flyte_branch=None,
        parameter_mapping=True,
    ):
        non_none_entities = list(filter(None, [flyte_task, flyte_workflow, flyte_launch_plan, flyte_branch]))
        if len(non_none_entities) != 1:
            raise _user_exceptions.FlyteAssertion(
                "An Flyte node must have one underlying entity specified at once.  Received the following "
                "entities: {}".format(non_none_entities)
            )

        workflow_node = None
        if flyte_workflow is not None:
            workflow_node = _component_nodes.FlyteWorkflowNode(flyte_workflow=flyte_workflow)
        elif flyte_launch_plan is not None:
            workflow_node = _component_nodes.FlyteWorkflowNode(flyte_launch_plan=flyte_launch_plan)

        super(FlyteNode, self).__init__(
            id=_dnsify(id) if id else None,
            metadata=metadata,
            inputs=bindings,
            upstream_node_ids=[n.id for n in upstream_nodes],
            output_aliases=[],
            task_node=_component_nodes.FlyteTaskNode(flyte_task) if flyte_task else None,
            workflow_node=workflow_node,
            branch_node=flyte_branch,
        )
        self._upstream = upstream_nodes

    @classmethod
    def promote_from_model(
        cls,
        model: _workflow_model.Node,
        sub_workflows: Optional[Dict[_identifier.Identifier, _workflow_model.WorkflowTemplate]],
        tasks: Optional[Dict[_identifier.Identifier, _task_model.TaskTemplate]],
    ) -> "FlyteNode":
        id = model.id
        if id in {_constants.START_NODE_ID, _constants.END_NODE_ID}:
            _logging.warning(f"Should not call promote from model on a start node or end node {model}")
            return None

        flyte_task_node, flyte_workflow_node = None, None
        if model.task_node is not None:
            flyte_task_node = _component_nodes.FlyteTaskNode.promote_from_model(model.task_node, tasks)
        elif model.workflow_node is not None:
            flyte_workflow_node = _component_nodes.FlyteWorkflowNode.promote_from_model(
                model.workflow_node, sub_workflows, tasks
            )
        else:
            raise _system_exceptions.FlyteSystemException("Bad Node model, neither task nor workflow detected.")

        # When WorkflowTemplate models (containing node models) are returned by Admin, they've been compiled with a
        # start node. In order to make the promoted FlyteWorkflow look the same, we strip the start-node text back out.
        for model_input in model.inputs:
            if (
                model_input.binding.promise is not None
                and model_input.binding.promise.node_id == _constants.START_NODE_ID
            ):
                model_input.binding.promise._node_id = _constants.GLOBAL_INPUT_NODE_ID

        if flyte_task_node is not None:
            return cls(
                id=id,
                upstream_nodes=[],  # set downstream, model doesn't contain this information
                bindings=model.inputs,
                metadata=model.metadata,
                flyte_task=flyte_task_node.flyte_task,
            )
        elif flyte_workflow_node is not None:
            if flyte_workflow_node.flyte_workflow is not None:
                return cls(
                    id=id,
                    upstream_nodes=[],  # set downstream, model doesn't contain this information
                    bindings=model.inputs,
                    metadata=model.metadata,
                    flyte_workflow=flyte_workflow_node.flyte_workflow,
                )
            elif flyte_workflow_node.flyte_launch_plan is not None:
                return cls(
                    id=id,
                    upstream_nodes=[],  # set downstream, model doesn't contain this information
                    bindings=model.inputs,
                    metadata=model.metadata,
                    flyte_launch_plan=flyte_workflow_node.flyte_launch_plan,
                )
            raise _system_exceptions.FlyteSystemException(
                "Bad FlyteWorkflowNode model, both launch plan and workflow are None"
            )
        raise _system_exceptions.FlyteSystemException("Bad FlyteNode model, both task and workflow nodes are empty")

    @property
    def upstream_nodes(self) -> List["FlyteNode"]:
        return self._upstream

    @property
    def upstream_node_ids(self) -> List[str]:
        return list(sorted(n.id for n in self.upstream_nodes))

    @property
    def outputs(self) -> Dict[str, NodeOutput]:
        return self._outputs

    def assign_id_and_return(self, id: str):
        if self.id:
            raise _user_exceptions.FlyteAssertion(
                f"Error assigning ID: {id} because {self} is already assigned. Has this node been ssigned to another "
                "workflow already?"
            )
        self._id = _dnsify(id) if id else None
        self._metadata.name = id
        return self

    def with_overrides(self, *args, **kwargs):
        # TODO: Implement overrides
        raise NotImplementedError("Overrides are not supported in Flyte yet.")

    def __repr__(self) -> str:
        return f"Node(ID: {self.id})"


class FlyteNodeExecution(_node_execution_models.NodeExecution, _artifact_mixin.ExecutionArtifact):
    def __init__(self, *args, **kwargs):
        super(FlyteNodeExecution, self).__init__(*args, **kwargs)
        self._task_executions = None
        self._subworkflow_node_executions = None
        self._inputs = None
        self._outputs = None
        self._interface = None

    @property
    def task_executions(self) -> List["flytekit.remote.tasks.executions.FlyteTaskExecution"]:
        return self._task_executions or []

    @property
    def subworkflow_node_executions(self) -> Dict[str, "flytekit.remote.nodes.FlyteNodeExecution"]:
        return (
            {}
            if self._subworkflow_node_executions is None
            else {n.id.node_id: n for n in self._subworkflow_node_executions}
        )

    @property
    def executions(self) -> List[_artifact_mixin.ExecutionArtifact]:
        return self.task_executions or self._subworkflow_node_executions or []

    @property
    def inputs(self) -> Dict[str, Any]:
        """
        Returns the inputs to the execution in the standard python format as dictated by the type engine.
        """
        if self._inputs is None:
            client = _flyte_engine.get_client()
            node_execution_data = client.get_node_execution_data(self.id)

            # Inputs are returned inline unless they are too big, in which case a url blob pointing to them is returned.
            input_map: _literal_models.LiteralMap = _literal_models.LiteralMap({})
            if bool(node_execution_data.full_inputs.literals):
                input_map = node_execution_data.full_inputs
            elif node_execution_data.inputs.bytes > 0:
                with _common_utils.AutoDeletingTempDir() as tmp_dir:
                    tmp_name = _os.path.join(tmp_dir.name, "inputs.pb")
                    _data_proxy.Data.get_data(node_execution_data.inputs.url, tmp_name)
                    input_map = _literal_models.LiteralMap.from_flyte_idl(
                        _common_utils.load_proto_from_file(_literals_pb2.LiteralMap, tmp_name)
                    )

            self._inputs = TypeEngine.literal_map_to_kwargs(
                ctx=FlyteContextManager.current_context(),
                lm=input_map,
                python_types=TypeEngine.guess_python_types(self.interface.inputs),
            )
        return self._inputs

    @property
    def outputs(self) -> Dict[str, Any]:
        """
        Returns the outputs to the execution in the standard python format as dictated by the type engine.

        :raises: ``FlyteAssertion`` error if execution is in progress or execution ended in error.
        """
        if not self.is_complete:
            raise _user_exceptions.FlyteAssertion(
                "Please wait until the node execution has completed before requesting the outputs."
            )
        if self.error:
            raise _user_exceptions.FlyteAssertion("Outputs could not be found because the execution ended in failure.")

        if self._outputs is None:
            client = _flyte_engine.get_client()
            execution_data = client.get_node_execution_data(self.id)

            # Outputs are returned inline unless they are too big, in which case a url blob pointing to them is returned.
            output_map: _literal_models.LiteralMap = _literal_models.LiteralMap({})
            if bool(execution_data.full_outputs.literals):
                output_map = execution_data.full_outputs
            elif execution_data.outputs.bytes > 0:
                with _common_utils.AutoDeletingTempDir() as tmp_dir:
                    tmp_name = _os.path.join(tmp_dir.name, "outputs.pb")
                    _data_proxy.Data.get_data(execution_data.outputs.url, tmp_name)
                    output_map = _literal_models.LiteralMap.from_flyte_idl(
                        _common_utils.load_proto_from_file(_literals_pb2.LiteralMap, tmp_name)
                    )

            self._outputs = TypeEngine.literal_map_to_kwargs(
                ctx=FlyteContextManager.current_context(),
                lm=output_map,
                python_types=TypeEngine.guess_python_types(self.interface.outputs),
            )
        return self._outputs

    @property
    def error(self) -> _execution_models.ExecutionError:
        """
        If execution is in progress, raise an exception. Otherwise, return None if no error was present upon
        reaching completion.
        """
        if not self.is_complete:
            raise _user_exceptions.FlyteAssertion(
                "Please wait until the node execution has completed before requesting error information."
            )
        return self.closure.error

    @property
    def is_complete(self) -> bool:
        """Whether or not the execution is complete."""
        return self.closure.phase in {
            _execution_models.NodeExecutionPhase.ABORTED,
            _execution_models.NodeExecutionPhase.FAILED,
            _execution_models.NodeExecutionPhase.SKIPPED,
            _execution_models.NodeExecutionPhase.SUCCEEDED,
            _execution_models.NodeExecutionPhase.TIMED_OUT,
        }

    @classmethod
    def promote_from_model(cls, base_model: _node_execution_models.NodeExecution) -> "FlyteNodeExecution":
        return cls(
            closure=base_model.closure, id=base_model.id, input_uri=base_model.input_uri, metadata=base_model.metadata
        )

    @property
    def interface(self) -> "flytekit.remote.interface.TypedInterface":
        """
        Return the interface of the task or subworkflow associated with this node execution.
        """
        if self._interface is None:
            from flytekit.remote.remote import FlyteRemote

            remote = FlyteRemote()

            if not self.metadata.is_parent_node:
                # if not a parent node, assume a task execution node
                task_id = self.task_executions[0].id.task_id
                task = remote.fetch_task(task_id.project, task_id.domain, task_id.name, task_id.version)
                self._interface = task.interface
            else:
                # otherwise assume the node is associated with a subworkflow
                client = _flyte_engine.get_client()

                # need to get the FlyteWorkflow associated with this node execution (self), so we need to fetch the
                # parent workflow and iterate through the parent's FlyteNodes to get the the FlyteWorkflow object
                # representing the subworkflow. This allows us to get the interface for guessing the types of the
                # inputs/outputs.
                lp_id = client.get_execution(self.id.execution_id).spec.launch_plan
                workflow = remote.fetch_workflow(lp_id.project, lp_id.domain, lp_id.name, lp_id.version)
                flyte_subworkflow_node: FlyteNode = [n for n in workflow.nodes if n.id == self.id.node_id][0]
                self._interface = flyte_subworkflow_node.target.flyte_workflow.interface

        return self._interface

    def sync(self):
        """
        Syncs the state of the underlying execution artifact with the state observed by the platform.
        """
        if self.metadata.is_parent_node:
            if not self.is_complete or self._subworkflow_node_executions is None:
                self._subworkflow_node_executions = [
                    FlyteNodeExecution.promote_from_model(n)
                    for n in iterate_node_executions(
                        _flyte_engine.get_client(),
                        workflow_execution_identifier=self.id.execution_id,
                        unique_parent_id=self.id.node_id,
                    )
                ]
        else:
            if not self.is_complete or self._task_executions is None:
                self._task_executions = [
                    FlyteTaskExecution.promote_from_model(t)
                    for t in iterate_task_executions(_flyte_engine.get_client(), self.id)
                ]

        self._sync_closure()
        for execution in self.executions:
            execution.sync()

    def _sync_closure(self):
        """
        Syncs the closure of the underlying execution artifact with the state observed by the platform.
        """
        self._closure = _flyte_engine.get_client().get_node_execution(self.id).closure
