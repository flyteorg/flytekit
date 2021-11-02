import os as _os

import six as _six
from flyteidl.core import literals_pb2 as _literals_pb2

from flytekit.clients.helpers import iterate_node_executions as _iterate_node_executions
from flytekit.common import nodes as _nodes
from flytekit.common import sdk_bases as _sdk_bases
from flytekit.common import utils as _common_utils
from flytekit.common.core import identifier as _core_identifier
from flytekit.common.exceptions import user as _user_exceptions
from flytekit.common.mixins import artifact as _artifact
from flytekit.common.types import helpers as _type_helpers
from flytekit.engines.flyte import engine as _flyte_engine
from flytekit.interfaces.data import data_proxy as _data_proxy
from flytekit.models import execution as _execution_models
from flytekit.models import literals as _literal_models
from flytekit.models.core import execution as _core_execution_models


class SdkWorkflowExecution(
    _execution_models.Execution,
    _artifact.ExecutionArtifact,
    metaclass=_sdk_bases.ExtendedSdkType,
):
    def __init__(self, *args, **kwargs):
        super(SdkWorkflowExecution, self).__init__(*args, **kwargs)
        self._node_executions = None
        self._inputs = None
        self._outputs = None

    @property
    def node_executions(self):
        """
        :rtype: dict[Text, flytekit.common.nodes.SdkNodeExecution]
        """
        return self._node_executions or {}

    @property
    def inputs(self):
        """
        Returns the inputs to the execution in the standard Python format as dictated by the type engine.
        :rtype:  dict[Text, T]
        """
        if self._inputs is None:
            client = _flyte_engine.get_client()
            execution_data = client.get_execution_data(self.id)

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
        :rtype:  dict[Text, T] or None
        """
        if not self.is_complete:
            raise _user_exceptions.FlyteAssertion(
                "Please what until the node execution has completed before " "requesting the outputs."
            )
        if self.error:
            raise _user_exceptions.FlyteAssertion("Outputs could not be found because the execution ended in failure.")

        if self._outputs is None:
            client = _flyte_engine.get_client()

            execution_data = client.get_execution_data(self.id)
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
                "Please wait until a workflow has completed before checking for an " "error."
            )
        return self.closure.error

    @property
    def is_complete(self):
        """
        Dictates whether or not the execution is complete.
        :rtype: bool
        """
        return self.closure.phase in {
            _core_execution_models.WorkflowExecutionPhase.ABORTED,
            _core_execution_models.WorkflowExecutionPhase.FAILED,
            _core_execution_models.WorkflowExecutionPhase.SUCCEEDED,
            _core_execution_models.WorkflowExecutionPhase.TIMED_OUT,
        }

    @classmethod
    def promote_from_model(cls, base_model):
        """
        :param _execution_models.Execution base_model:
        :rtype: SdkWorkflowExecution
        """
        return cls(
            closure=base_model.closure,
            id=_core_identifier.WorkflowExecutionIdentifier.promote_from_model(base_model.id),
            spec=base_model.spec,
        )

    @classmethod
    def fetch(cls, project, domain, name):
        """
        :param Text project:
        :param Text domain:
        :param Text name:
        :rtype: SdkWorkflowExecution
        """
        wf_exec_id = _core_identifier.WorkflowExecutionIdentifier(project=project, domain=domain, name=name)
        admin_exec = _flyte_engine.get_client().get_execution(wf_exec_id)

        return cls.promote_from_model(admin_exec)

    def sync(self):
        """
        Syncs the state of the underlying execution artifact with the state observed by the platform.
        :rtype: None
        """
        if not self.is_complete or self._node_executions is None:
            self._sync_closure()
            self._node_executions = self.get_node_executions()

    def _sync_closure(self):
        """
        Syncs the closure of the underlying execution artifact with the state observed by the platform.
        :rtype: None
        """
        if not self.is_complete:
            client = _flyte_engine.get_client()
            self._closure = client.get_execution(self.id).closure

    def get_node_executions(self, filters=None):
        """
        :param list[flytekit.models.filters.Filter] filters:
        :rtype: dict[Text, flytekit.common.nodes.SdkNodeExecution]
        """
        client = _flyte_engine.get_client()
        node_exec_models = {v.id.node_id: v for v in _iterate_node_executions(client, self.id, filters=filters)}

        return {k: _nodes.SdkNodeExecution.promote_from_model(v) for k, v in _six.iteritems(node_exec_models)}

    def terminate(self, cause):
        """
        :param Text cause:
        """
        _flyte_engine.get_client().terminate_execution(self.id, cause)
