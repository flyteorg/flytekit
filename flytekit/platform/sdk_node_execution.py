import os as _os

from flyteidl.core import literals_pb2 as _literals_pb2

from platform.clients import iterate_task_executions as _iterate_task_executions
from flytekit.common import sdk_bases as _sdk_bases, utils as _common_utils
from flytekit.common.exceptions import user as _user_exceptions
from flytekit.common.mixins import artifact as _artifact_mixin
from flytekit.common.tasks import executions as _task_executions
from flytekit.common.types import helpers as _type_helpers
from flytekit.legacy.engines.flyte import engine as _flyte_engine
from flytekit.common.interfaces.data import data_proxy as _data_proxy
from flytekit.models import node_execution as _node_execution_models, literals as _literal_models
from flytekit.models.core import execution as _execution_models


class SdkNodeExecution(
    _node_execution_models.NodeExecution, _artifact_mixin.ExecutionArtifact, metaclass=_sdk_bases.ExtendedSdkType
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
        :rtype: list[flytekit.platform.sdk_workflow_execution.SdkWorkflowExecution]
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
            client = _flyte_engine.get_client()
            execution_data = client.get_node_execution_data(self.id)

            # Inputs are returned inline unless they are too big, in which case a url blob pointing to them is returned.
            if bool(execution_data.full_inputs.literals):
                input_map = execution_data.full_inputs
            elif execution_data.inputs.bytes > 0:
                with _common_utils.AutoDeletingTempDir() as t:
                    tmp_name = _os.path.join(t.name, "inputs.pb")
                    _data_proxy.Data.get_data(execution_data.inputs.url, tmp_name)
                    input_map = _literal_models.LiteralMap.from_flyte_idl(
                        _common_utils.load_proto_from_file(_literals_pb2.LiteralMap, tmp_name)
                    )
            else:
                input_map = _literal_models.LiteralMap({})

            self._inputs = _type_helpers.unpack_literal_map_to_sdk_python_std(input_map)
        return self._inputs

    @property
    def outputs(self):
        """
        Returns the outputs to the execution in the standard Python format as dictated by the type engine.  If the
        execution ended in error or the execution is in progress, an exception will be raised.
        :rtype: dict[Text, T]
        """
        if not self.is_complete:
            raise _user_exceptions.FlyteAssertion(
                "Please what until the node execution has completed before requesting the outputs."
            )
        if self.error:
            raise _user_exceptions.FlyteAssertion("Outputs could not be found because the execution ended in failure.")

        if self._outputs is None:
            client = _flyte_engine.get_client()
            execution_data = client.get_node_execution_data(self.id)

            # Outputs are returned inline unless they are too big, in which case a url blob pointing to them is returned.
            if bool(execution_data.full_outputs.literals):
                output_map = execution_data.full_outputs

            elif execution_data.outputs.bytes > 0:
                with _common_utils.AutoDeletingTempDir() as t:
                    tmp_name = _os.path.join(t.name, "outputs.pb")
                    _data_proxy.Data.get_data(execution_data.outputs.url, tmp_name)
                    output_map = _literal_models.LiteralMap.from_flyte_idl(
                        _common_utils.load_proto_from_file(_literals_pb2.LiteralMap, tmp_name)
                    )
            else:
                output_map = _literal_models.LiteralMap({})

            self._outputs = _type_helpers.unpack_literal_map_to_sdk_python_std(output_map)
        return self._outputs

    @property
    def error(self):
        """
        If execution is in progress, raise an exception.  Otherwise, return None if no error was present upon
        reaching completion.
        :rtype: flytekit.models.core.execution.ExecutionError or None
        """
        if not self.is_complete:
            raise _user_exceptions.FlyteAssertion(
                "Please what until the node execution has completed before requesting error information."
            )
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
        return cls(closure=base_model.closure, id=base_model.id, input_uri=base_model.input_uri)

    def sync(self):
        """
        Syncs the state of this object with that held by the platform.
        :rtype: None
        """
        if not self.is_complete or self.task_executions is not None:
            client = _flyte_engine.get_client()
            self._closure = client.get_node_execution(self.id).closure
            task_executions = list(_iterate_task_executions(client, self.id))
            self._task_executions = [_task_executions.SdkTaskExecution.promote_from_model(te) for te in task_executions]
            # TODO: Sub-workflows too once implemented

    def _sync_closure(self):
        """
        Syncs the closure of the underlying execution artifact with the state observed by the platform.
        :rtype: None
        """
        client = _flyte_engine.get_client()
        self._closure = client.get_node_execution(self.id).closure