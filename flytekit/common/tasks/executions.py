import os as _os

import six as _six
from flyteidl.core import literals_pb2 as _literals_pb2

from flytekit.clients.helpers import iterate_node_executions as _iterate_node_executions
from flytekit.common import sdk_bases as _sdk_bases
from flytekit.common import utils as _common_utils
from flytekit.common.exceptions import user as _user_exceptions
from flytekit.common.mixins import artifact as _artifact_mixin
from flytekit.common.types import helpers as _type_helpers
from flytekit.engines.flyte import engine as _flyte_engine
from flytekit.interfaces.data import data_proxy as _data_proxy
from flytekit.models import literals as _literal_models
from flytekit.models.admin import task_execution as _task_execution_model
from flytekit.models.core import execution as _execution_models


class SdkTaskExecution(
    _task_execution_model.TaskExecution, _artifact_mixin.ExecutionArtifact, metaclass=_sdk_bases.ExtendedSdkType
):
    def __init__(self, *args, **kwargs):
        super(SdkTaskExecution, self).__init__(*args, **kwargs)
        self._inputs = None
        self._outputs = None

    @property
    def is_complete(self):
        """
        Dictates whether or not the execution is complete.
        :rtype: bool
        """
        return self.closure.phase in {
            _execution_models.TaskExecutionPhase.ABORTED,
            _execution_models.TaskExecutionPhase.FAILED,
            _execution_models.TaskExecutionPhase.SUCCEEDED,
        }

    @property
    def inputs(self):
        """
        Returns the inputs of the task execution in the standard Python format that is produced by
        the type engine.
        :rtype: dict[Text, T]
        """
        if self._inputs is None:
            client = _flyte_engine.get_client()
            execution_data = client.get_task_execution_data(self.id)

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
        Returns the outputs of the task execution, if available, in the standard Python format that is produced by
        the type engine. If not available, perhaps due to execution being in progress or an error being produced,
        this will raise an exception.
        :rtype: dict[Text, T]
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
                "Please what until the task execution has completed before requesting error information."
            )
        return self.closure.error

    def get_child_executions(self, filters=None):
        """
        :param list[flytekit.models.filters.Filter] filters:
        :rtype: dict[Text, flytekit.common.nodes.SdkNodeExecution]
        """
        from flytekit.common import nodes as _nodes

        if not self.is_parent:
            raise _user_exceptions.FlyteAssertion("Only task executions marked with 'is_parent' have child executions.")
        client = _flyte_engine.get_client()
        models = {
            v.id.node_id: v
            for v in _iterate_node_executions(client, task_execution_identifier=self.id, filters=filters)
        }

        return {k: _nodes.SdkNodeExecution.promote_from_model(v) for k, v in _six.iteritems(models)}

    @classmethod
    def promote_from_model(cls, base_model):
        """
        :param _task_execution_model.TaskExecution base_model:
        :rtype: SdkTaskExecution
        """
        return cls(
            closure=base_model.closure,
            id=base_model.id,
            input_uri=base_model.input_uri,
            is_parent=base_model.is_parent,
        )

    def sync(self):
        self._sync_closure()

    def _sync_closure(self):
        """
        Syncs the closure of the underlying execution artifact with the state observed by the platform.
        :rtype: None
        """
        client = _flyte_engine.get_client()
        self._closure = client.get_task_execution(self.id).closure
