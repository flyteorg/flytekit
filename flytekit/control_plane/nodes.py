import logging as _logging
import os as _os
from typing import Any, Dict, List, Optional

from flyteidl.core import literals_pb2 as _literals_pb2

from flytekit.clients.helpers import iterate_task_executions as _iterate_task_executions
from flytekit.common import constants as _constants
from flytekit.common import utils as _common_utils
from flytekit.common.exceptions import system as _system_exceptions
from flytekit.common.exceptions import user as _user_exceptions
from flytekit.common.mixins import artifact as _artifact_mixin
from flytekit.common.mixins import hash as _hash_mixin
from flytekit.common.utils import _dnsify
from flytekit.control_plane import component_nodes as _component_nodes
from flytekit.control_plane import identifier as _identifier
from flytekit.control_plane.tasks import executions as _task_executions
from flytekit.core.promise import NodeOutput
from flytekit.engines.flyte import engine as _flyte_engine
from flytekit.interfaces.data import data_proxy as _data_proxy
from flytekit.models import literals as _literal_models
from flytekit.models import node_execution as _node_execution_models
from flytekit.models import task as _task_model
from flytekit.models.core import execution as _execution_models
from flytekit.models.core import workflow as _workflow_model


class FlyteNode(_hash_mixin.HashOnReferenceMixin, _workflow_model.Node):
    def __init__(
        self,
        id,
        upstream_nodes,
        bindings,
        metadata,
        flyte_task: "flytekit.control_plan.tasks.task.FlyteTask" = None,
        flyte_workflow: "flytekit.control_plane.workflow.FlyteWorkflow" = None,
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
                    bindings=models.inputs,
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
        return f"Node(ID: {self.id} Executable: {self._executable_flyte_object})"


class FlyteNodeExecution(_node_execution_models.NodeExecution, _artifact_mixin.ExecutionArtifact):
    def __init__(self, *args, **kwargs):
        super(FlyteNodeExecution, self).__init__(*args, **kwargs)
        self._task_executions = None
        self._workflow_executions = None
        self._inputs = None
        self._outputs = None

    @property
    def task_executions(self) -> List["flytekit.control_plane.tasks.executions.FlyteTaskExecution"]:
        return self._task_executions or []

    @property
    def workflow_executions(self) -> List["flytekit.control_plane.workflow_executions.FlyteWorkflowExecution"]:
        return self._workflow_executions or []

    @property
    def executions(self) -> _artifact_mixin.ExecutionArtifact:
        return self.task_executions or self.workflow_executions or []

    @property
    def inputs(self) -> Dict[str, Any]:
        """
        Returns the inputs to the execution in the standard python format as dicatated by the type engine.
        """
        if self._inputs is None:
            client = _flyte_engine.get_client()
            execution_data = client.get_node_execution_data(self.id)

            # Inputs are returned inline unless they are too big, in which case a url blob pointing to them is returned.
            input_map: _literal_models.LiteralMap = _literal_models.LiteralMap({})
            if bool(execution_data.full_inputs.literals):
                input_map = execution_data.full_inputs
            elif execution_data.inputs.bytes > 0:
                with _common_utils.AutoDeletingTempDir() as tmp_dir:
                    tmp_name = _os.path.join(tmp_dir.name, "inputs.pb")
                    _data_proxy.Data.get_data(execution_data.inputs.url, tmp_name)
                    input_map = _literal_models.LiteralMap.from_flyte_idl(
                        _common_utils.load_proto_from_file(_literals_pb2.LiteralMap, tmp_name)
                    )

            # TODO: need to convert flyte literals to python types. For now just use literals
            # self._inputs = TypeEngine.literal_map_to_kwargs(ctx=FlyteContext.current_context(), lm=input_map)
            self._inputs = input_map
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
            # TODO: need to convert flyte literals to python types. For now just use literals
            # self._outputs = TypeEngine.literal_map_to_kwargs(ctx=FlyteContext.current_context(), lm=output_map)
            self._outputs = output_map
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
        return cls(closure=base_model.closure, id=base_model.id, input_uri=base_model.input_uri)

    def sync(self):
        """
        Syncs the state of the underlying execution artifact with the state observed by the platform.
        """
        if not self.is_complete or self.task_executions is not None:
            client = _flyte_engine.get_client()
            self._closure = client.get_node_execution(self.id).closure
            self._task_executions = [
                _task_executions.FlyteTaskExecution.promote_from_model(t)
                for t in _iterate_task_executions(client, self.id)
            ]
            # TODO: sync sub-workflows as well

    def _sync_closure(self):
        """
        Syncs the closure of the underlying execution artifact with the state observed by the platform.
        """
        self._closure = _flyte_engine.get_client().get_node_execution(self.id).closure
