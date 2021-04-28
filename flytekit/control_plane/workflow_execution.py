import os as _os
from typing import Any, Dict, List

from flyteidl.core import literals_pb2 as _literals_pb2

from flytekit.clients.helpers import iterate_node_executions as _iterate_node_executions
from flytekit.common import utils as _common_utils
from flytekit.common.exceptions import user as _user_exceptions
from flytekit.common.mixins import artifact as _artifact
from flytekit.control_plane import identifier as _core_identifier
from flytekit.control_plane import nodes as _nodes
from flytekit.engines.flyte import engine as _flyte_engine
from flytekit.interfaces.data import data_proxy as _data_proxy
from flytekit.models import execution as _execution_models
from flytekit.models import filters as _filter_models
from flytekit.models import literals as _literal_models
from flytekit.models.core import execution as _core_execution_models


class FlyteWorkflowExecution(_execution_models.Execution, _artifact.ExecutionArtifact):
    def __init__(self, *args, **kwargs):
        super(FlyteWorkflowExecution, self).__init__(*args, **kwargs)
        self._node_executions = None
        self._inputs = None
        self._outputs = None

    @property
    def node_executions(self) -> Dict[str, _nodes.FlyteNodeExecution]:
        return self._node_executions or {}

    @property
    def inputs(self) -> Dict[str, Any]:
        """
        Returns the inputs to the execution in the standard python format as dictated by the type engine.
        """
        if self._inputs is None:
            client = _flyte_engine.get_client()
            execution_data = client.get_execution_data(self.id)

            # Inputs are returned inline unless they are too big, in which case a url blob pointing to them is returned.
            input_map: LiteralMap = _literal_models.LiteralMap({})
            if bool(execution_data.full_inputs.literals):
                input_map = execution_data.full_inputs
            elif execution_data.inputs.bytes > 0:
                with _common_utils.AutoDeletingTempDir() as tmp_dir:
                    tmp_name = _os.path.join(tmp_dir.name, "inputs.pb")
                    _data_proxy.Data.get_data(execution_data.inputs.url, tmp_name)
                    input_map = _literal_models.LiteralMap.from_flyte_idl(
                        _common_utils.load_proto_from_file(_literals_pb2.Literalmap, tmp_name)
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
            execution_data = client.get_execution_data(self.id)
            # Outputs are returned inline unless they are too big, in which case a url blob pointing to them is returned.
            output_map: LiteralMap = _literal_models.LiteralMap({})
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
    def error(self) -> _core_execution_models.ExecutionError:
        """
        If execution is in progress, raise an exception.  Otherwise, return None if no error was present upon
        reaching completion.
        """
        if not self.is_complete:
            raise _user_exceptions.FlyteAssertion(
                "Please wait until a workflow has completed before checking for an " "error."
            )
        return self.closure.error

    @property
    def is_complete(self) -> bool:
        """
        Whether or not the execution is complete.
        """
        return self.closure.phase in {
            _core_execution_models.WorkflowExecutionPhase.ABORTED,
            _core_execution_models.WorkflowExecutionPhase.FAILED,
            _core_execution_models.WorkflowExecutionPhase.SUCCEEDED,
            _core_execution_models.WorkflowExecutionPhase.TIMED_OUT,
        }

    @classmethod
    def promote_from_model(cls, base_model: _execution_models.Execution) -> "FlyteWorkflowExecution":
        return cls(
            closure=base_model.closure,
            id=_core_identifier.WorkflowExecutionIdentifier.promote_from_model(base_model.id),
            spec=base_model.spec,
        )

    @classmethod
    def fetch(cls, project: str, domain: str, name: str) -> "FlyteWorkflowExecution":
        return cls.promote_from_model(
            _flyte_engine.get_client().get_execution(
                _core_identifier.WorkflowExecutionIdentifier(project=project, domain=domain, name=name)
            )
        )

    def sync(self):
        """
        Syncs the state of the underlying execution artifact with the state observed by the platform.
        """
        if not self.is_complete or self._node_executions is None:
            self._sync_closure()
            self._node_executions = self.get_node_executions()

    def _sync_closure(self):
        if not self.is_complete:
            client = _flyte_engine.get_client()
            self._closure = client.get_execution(self.id).closure

    def get_node_executions(self, filters: List[_filter_models.Filter] = None) -> Dict[str, _nodes.FlyteNodeExecution]:
        client = _flyte_engine.get_client()
        return {
            node.id.node_id: _nodes.FlyteNodeExecution.promote_from_model(node)
            for node in _iterate_node_executions(client, self.id, filters=filters)
        }

    def terminate(self, cause: str):
        _flyte_engine.get_client().terminate_execution(self.id, cause)
