from __future__ import absolute_import

import uuid as _uuid

import six as _six
from google.protobuf.json_format import MessageToDict as _MessageToDict

from flytekit.common import constants as _constants, nodes as _nodes, interface as _interface
from flytekit.common.exceptions import scopes as _exception_scopes
from flytekit.common.exceptions.user import FlyteTypeException as _FlyteTypeException, \
    FlyteValueException as _FlyteValueException
from flytekit.common.tasks import output as _task_output, sdk_runnable as _sdk_runnable, task as _base_task
from flytekit.common.tasks.mixins.executable_traits import function as _function_mixin, notebook as _notebook_mixin
from flytekit.common.types import helpers as _type_helpers
from flytekit.models import (
    qubole as _qubole,
    interface as _interface_model,
    literals as _literal_models,
    dynamic_job as _dynamic_job
)
from flytekit.models.core import workflow as _workflow_model

ALLOWED_TAGS_COUNT = int(6)
MAX_TAG_LENGTH = int(20)


class _SdkHiveTask(_sdk_runnable.SdkRunnableTask):
    """
    This class includes the additional logic for building a task that executes as a batch hive task.
    """

    def __init__(
            self,
            cluster_label=None,
            tags=None,
            **kwargs
    ):
        """
        :param Text cluster_label:
        :param list[Text] tags:
        :param kwargs: See _sdk_runnable.SdkRunnableTask
        """
        super(_SdkHiveTask, self).__init__(**kwargs)
        self._validate_task_parameters(cluster_label, tags)
        self._cluster_label = cluster_label
        self._tags = tags

    def _generate_plugin_objects(self, context, inputs_dict):
        """
        Runs user code and and produces hive queries
        :param flytekit.engines.common.EngineContext context:
        :param dict[Text,T] inputs_dict:
        :rtype: list[_qubole.QuboleHiveJob]
        """
        queries_from_task = super(_SdkHiveTask, self)._execute_user_code(context, inputs_dict) or []
        if not isinstance(queries_from_task, list):
            queries_from_task = [queries_from_task]

        self._validate_queries(queries_from_task)
        plugin_objects = []

        for q in queries_from_task:
            hive_query = _qubole.HiveQuery(query=q, timeout_sec=self.metadata.timeout.seconds,
                                           retry_count=self.metadata.retries.retries)

            # TODO: Remove this after all users of older SDK versions that did the single node, multi-query pattern are
            #       deprecated. This is only here for backwards compatibility - in addition to writing the query to the
            #       query field, we also construct a QueryCollection with only one query. This will ensure that the
            #       older plugin will continue to work.
            query_collection = _qubole.HiveQueryCollection([hive_query])

            plugin_objects.append(_qubole.QuboleHiveJob(hive_query, self._cluster_label, self._tags,
                                                        query_collection=query_collection))

        return plugin_objects

    @staticmethod
    def _validate_task_parameters(cluster_label, tags):
        if not (cluster_label is None or isinstance(cluster_label, (str, _six.text_type))):
            raise _FlyteTypeException(
                type(cluster_label),
                {str, _six.text_type},
                additional_msg="cluster_label for a hive task must be in text format",
                received_value=cluster_label
            )
        if tags is not None:
            if not (isinstance(tags, list) and all(isinstance(tag, (str, _six.text_type)) for tag in tags)):
                raise _FlyteTypeException(
                    type(tags),
                    [],
                    additional_msg="tags for a hive task must be in 'list of text' format",
                    received_value=tags
                )
            if len(tags) > ALLOWED_TAGS_COUNT:
                raise _FlyteValueException(len(tags), "number of tags must be less than {}".format(ALLOWED_TAGS_COUNT))
            if not all(len(tag) for tag in tags):
                raise _FlyteValueException(tags, "length of a tag must be less than {} chars".format(MAX_TAG_LENGTH))

    @staticmethod
    def _validate_queries(queries_from_task):
        for query_from_task in queries_from_task or []:
            if not isinstance(query_from_task, (str, _six.text_type)):
                raise _FlyteTypeException(
                    type(query_from_task),
                    {str, _six.text_type},
                    additional_msg="All queries returned from a Hive task must be in text format.",
                    received_value=query_from_task
                )

    def _produce_dynamic_job_spec(self, context, inputs):
        """
        Runs user code and and produces future task nodes to run sub-tasks.
        :param context:
        :param flytekit.models.literals.LiteralMap literal_map inputs:
        :rtype: flytekit.models.dynamic_job.DynamicJobSpec
        """
        inputs_dict = _type_helpers.unpack_literal_map_to_sdk_python_std(inputs, {
            k: _type_helpers.get_sdk_type_from_literal_type(v.type) for k, v in _six.iteritems(self.interface.inputs)
        })
        outputs_dict = {
            name: _task_output.OutputReference(_type_helpers.get_sdk_type_from_literal_type(variable.type))
            for name, variable in _six.iteritems(self.interface.outputs)
        }

        # Add outputs to inputs
        inputs_dict.update(outputs_dict)

        nodes = []
        tasks = []
        # One node per query
        generated_queries = self._generate_plugin_objects(context, inputs_dict)

        # Create output bindings always - this has to happen after user code has run
        output_bindings = [_literal_models.Binding(var=name, binding=_interface.BindingData.from_python_std(
            b.sdk_type.to_flyte_literal_type(), b.value))
                           for name, b in _six.iteritems(outputs_dict)]

        i = 0
        for quboleHiveJob in generated_queries:
            hive_job_node = _create_hive_job_node(
                "HiveQuery_{}".format(i),
                quboleHiveJob.to_flyte_idl(),
                self.metadata
            )
            nodes.append(hive_job_node)
            tasks.append(hive_job_node.executable_sdk_object)
            i += 1

        dynamic_job_spec = _dynamic_job.DynamicJobSpec(
            min_successes=len(nodes),
            tasks=tasks,
            nodes=nodes,
            outputs=output_bindings,
            subworkflows=[])

        return dynamic_job_spec

    # TODO: Re-write
    @_exception_scopes.system_entry_point
    def execute(self, context, inputs):
        """
        Executes hive batch task's user code and produces futures file as well as all sub-task inputs.pb files.

        :param flytekit.engines.common.EngineContext context:
        :param flytekit.models.literals.LiteralMap inputs:
        :rtype: dict[Text, flytekit.models.common.FlyteIdlEntity]
        :returns: This function must return a dictionary mapping 'filenames' to Flyte Interface Entities.  These
            entities will be used by the engine to pass data from node to node, populate metadata, etc. etc..  Each
            engine will have different behavior.  For instance, the Flyte engine will upload the entities to a remote
            working directory (with the names provided), which will in turn allow Flyte Propeller to push along the
            workflow.  Where as local engine will merely feed the outputs directly into the next node.
        """
        spec = self._produce_dynamic_job_spec(context, inputs)
        generated_files = {}

        # If no queries were produced, then the spec should not have any nodes, in which case we just produce an
        # outputs file like any other single-step tasks.
        if len(spec.nodes) == 0:
            return {
                _constants.OUTPUT_FILE_NAME: _literal_models.LiteralMap(
                    literals={binding.var: binding.binding.to_literal_model() for binding in spec.outputs})
            }
        else:
            generated_files.update({
                _constants.FUTURES_FILE_NAME: spec
            })

            return generated_files


def _create_hive_job_node(name, hive_job, metadata):
    """
    :param Text name:
    :param _qubole.QuboleHiveJob hive_job: Hive job spec
    :param flytekit.models.task.TaskMetadata metadata: This contains information needed at runtime to determine
        behavior such as whether or not outputs are discoverable, timeouts, and retries.
    :rtype: _nodes.SdkNode:
    """
    return _nodes.SdkNode(
        id=_six.text_type(_uuid.uuid4()),
        upstream_nodes=[],
        bindings=[],
        metadata=_workflow_model.NodeMetadata(name, metadata.timeout, _literal_models.RetryStrategy(0)),
        sdk_task=_SdkHiveJob(hive_job, metadata)
    )


class _SdkHiveJob(_base_task.SdkTask):
    """
    This class encapsulates the hive-job that is submitted to the Qubole Operator.

    """

    def __init__(
            self,
            hive_job,
            metadata,
    ):
        """
        :param _qubole.QuboleHiveJob hive_job: Hive job spec
        :param TaskMetadata metadata: This contains information needed at runtime to determine behavior such as
            whether or not outputs are discoverable, timeouts, and retries.
        """
        super(_SdkHiveJob, self).__init__(
            _constants.SdkTaskType.HIVE_JOB,
            metadata,
            # Individual hive tasks never take anything, or return anything. They just run a query that's already
            # got the location set.
            _interface_model.TypedInterface({}, {}),
            _MessageToDict(hive_job),
        )


class HiveFunctionTask(_function_mixin.WrappedFunctionTask, _SdkHiveTask):
    pass


class HiveNotebookTask(_notebook_mixin.NotebookTask, _SdkHiveTask):
    pass
