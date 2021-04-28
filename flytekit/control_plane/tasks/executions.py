from typing import Any, Dict, Optional

from flytekit.clients.helpers import iterate_node_executions as _iterate_node_executions
from flytekit.common import utils as _common_utils
from flytekit.common.exceptions import user as _user_exceptions
from flytekit.common.mixins import artifact as _artifact_mixin
from flytekit.engines.flyte import engine as _flyte_engine
from flytekit.models import literals as _literal_models
from flytekit.models.admin import task_execution as _task_execution_model
from flytekit.models.core import execution as _execution_models


class FlyteTaskExecution(_task_execution_model.TaskExecution, _artifact_mixin.ExecutionArtifact):
    def __init__(self, *args, **kwargs):
        super(FlyteTaskExecution, self).__init__(*args, **kwargs)
        self._inputs = None
        self._outputs = None

    @property
    def is_complete(self) -> bool:
        """Whether or not the execution is complete."""
        return self.closure.phase in {
            _execution_models.TaskExecutionPhase.ABORTED,
            _execution_models.TaskExecutionPhase.FAILED,
            _execution_models.TaskExecutionPhase.SUCCEEDED,
        }

    @property
    def inputs(self) -> Dict[str, Any]:
        """
        Returns the inputs of the task execution in the standard Python format that is produced by
        the type engine.
        """
        if self._inputs is None:
            client = _flyte_engine.get_client()
            execution_data = client.get_task_execution_data(self.id)

            # Inputs are returned inline unless they are too big, in which case a url blob pointing to them is returned.
            input_map = _literal_models.LiteralMap({})
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
        Returns the outputs of the task execution, if available, in the standard Python format that is produced by
        the type engine.

        :raises: ``FlyteAssertion`` error if execution is in progress or execution ended in error.
        """
        if not self.is_complete:
            raise _user_exceptions.FlyteAssertion(
                "Please what until the task execution has completed before requesting the outputs."
            )
        if self.error:
            raise _user_exceptions.FlyteAssertion("Outputs could not be found because the execution ended in failure.")

        if self._outputs is None:
            client = _flyte_engine.get_client()
            execution_data = client.get_task_execution_data(self.id)

            # Inputs are returned inline unless they are too big, in which case a url blob pointing to them is returned.
            output_map = _literal_models.LiteralMap({})
            if bool(execution_data.full_outputs.literals):
                output_map = execution_data.full_outputs
            elif execution_data.outputs.bytes > 0:
                with _common_utils.AutoDeletingTempDir() as t:
                    tmp_name = _os.path.join(t.name, "outputs.pb")
                    _data_proxy.Data.get_data(execution_data.outputs.url, tmp_name)
                    output_map = _literal_models.LiteralMap.from_flyte_idl(
                        _common_utils.load_proto_from_file(_literals_pb2.LiteralMap, tmp_name)
                    )

            # TODO: need to convert flyte literals to python types. For now just use literals
            # self._outputs = TypeEngine.literal_map_to_kwargs(ctx=FlyteContext.current_context(), lm=output_map)
            self._outputs = output_map
        return self._outputs

    @property
    def error(self) -> Optional[_execution_models.ExecutionError]:
        """
        If execution is in progress, raise an exception. Otherwise, return None if no error was present upon
        reaching completion.
        """
        if not self.is_complete:
            raise _user_exceptions.FlyteAssertion(
                "Please what until the task execution has completed before requesting error information."
            )
        return self.closure.error

    def get_child_executions(self, filters=None):
        from flytekit.control_plane import nodes as _nodes

        if not self.is_parent:
            raise _user_exceptions.FlyteAssertion("Only task executions marked with 'is_parent' have child executions.")
        client = _flyte_engine.get_client()
        models = {
            v.id.node_id: v
            for v in _iterate_node_executions(client, task_execution_identifier=self.id, filters=filters)
        }

        return {k: _nodes.FlyteNodeExecution.promote_from_model(v) for k, v in models.items()}

    @classmethod
    def promote_from_model(cls, base_model: _task_execution_model.TaskExecution) -> "FlyteTaskExecution":
        return cls(
            closure=base_model.closure,
            id=base_model.id,
            input_uri=base_model.input_uri,
            is_parent=base_model.is_parent,
        )

    def sync(self):
        """
        Syncs the state of the underlying execution artifact with the state observed by the platform.
        """
        self._sync_closure()

    def _sync_closure(self):
        """
        Syncs the closure of the underlying execution artifact with the state observed by the platform.
        """
        self._closure = _flyte_engine.get_client().get_task_execution(self.id).closure
