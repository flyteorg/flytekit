import uuid as _uuid

import six as _six
from google.protobuf.json_format import MessageToDict as _MessageToDict

from flytekit.common import constants as _constants
from flytekit.common import interface as _interface
from flytekit.common import nodes as _nodes
from flytekit.common.exceptions import scopes as _exception_scopes
from flytekit.common.exceptions.user import FlyteTypeException as _FlyteTypeException
from flytekit.common.exceptions.user import FlyteValueException as _FlyteValueException
from flytekit.common.tasks import output as _task_output
from flytekit.common.tasks import sdk_runnable as _sdk_runnable
from flytekit.common.tasks import task as _base_task
from flytekit.common.types import helpers as _type_helpers
from flytekit.models import dynamic_job as _dynamic_job
from flytekit.models import interface as _interface_model
from flytekit.models import literals as _literal_models
from flytekit.models import qubole as _qubole
from flytekit.models.core import workflow as _workflow_model

ALLOWED_TAGS_COUNT = int(6)
MAX_TAG_LENGTH = int(20)


class SdkHiveTask(_sdk_runnable.SdkRunnableTask):
    """
    This class includes the additional logic for building a task that executes as a batch hive task.
    """

    def __init__(
        self,
        task_function,
        task_type,
        discovery_version,
        retries,
        interruptible,
        deprecated,
        storage_request,
        cpu_request,
        gpu_request,
        memory_request,
        storage_limit,
        cpu_limit,
        gpu_limit,
        memory_limit,
        discoverable,
        timeout,
        cluster_label,
        tags,
        environment,
        cache_serializable,
    ):
        """
        :param task_function: Function container user code.  This will be executed via the SDK's engine.
        :param Text task_type: string describing the task type
        :param Text discovery_version: string describing the version for task discovery purposes
        :param int retries: Number of retries to attempt
        :param Text deprecated:
        :param Text storage_request:
        :param Text cpu_request:
        :param Text gpu_request:
        :param Text memory_request:
        :param Text storage_limit:
        :param Text cpu_limit:
        :param Text gpu_limit:
        :param Text memory_limit:
        :param bool discoverable:
        :param datetime.timedelta timeout:
        :param Text cluster_label:
        :param list[Text] tags:
        :param dict[Text, Text] environment:
        :param bool cache_serializable:
        """
        self._task_function = task_function
        super(SdkHiveTask, self).__init__(
            task_function,
            task_type,
            discovery_version,
            retries,
            interruptible,
            deprecated,
            storage_request,
            cpu_request,
            gpu_request,
            memory_request,
            storage_limit,
            cpu_limit,
            gpu_limit,
            memory_limit,
            discoverable,
            timeout,
            environment,
            cache_serializable,
            {},
        )
        self._validate_task_parameters(cluster_label, tags)
        self._cluster_label = cluster_label
        self._tags = tags

    def _generate_plugin_objects(self, context, inputs_dict):
        """
        Runs user code and and produces hive queries
        :param flytekit.engines.common.EngineContext context:
        :param dict[Text, T] inputs:
        :rtype: list[_qubole.QuboleHiveJob]
        """
        queries_from_task = super(SdkHiveTask, self)._execute_user_code(context, inputs_dict) or []
        if not isinstance(queries_from_task, list):
            queries_from_task = [queries_from_task]

        self._validate_queries(queries_from_task)
        plugin_objects = []

        for q in queries_from_task:
            hive_query = _qubole.HiveQuery(
                query=q,
                timeout_sec=self.metadata.timeout.seconds,
                retry_count=self.metadata.retries.retries,
            )

            # TODO: Remove this after all users of older SDK versions that did the single node, multi-query pattern are
            #       deprecated. This is only here for backwards compatibility - in addition to writing the query to the
            #       query field, we also construct a QueryCollection with only one query. This will ensure that the
            #       older plugin will continue to work.
            query_collection = _qubole.HiveQueryCollection([hive_query])

            plugin_objects.append(
                _qubole.QuboleHiveJob(
                    hive_query,
                    self._cluster_label,
                    self._tags,
                    query_collection=query_collection,
                )
            )

        return plugin_objects

    @staticmethod
    def _validate_task_parameters(cluster_label, tags):
        if not (cluster_label is None or isinstance(cluster_label, (str, _six.text_type))):
            raise _FlyteTypeException(
                type(cluster_label),
                {str, _six.text_type},
                additional_msg="cluster_label for a hive task must be in text format",
                received_value=cluster_label,
            )
        if tags is not None:
            if not (isinstance(tags, list) and all(isinstance(tag, (str, _six.text_type)) for tag in tags)):
                raise _FlyteTypeException(
                    type(tags),
                    [],
                    additional_msg="tags for a hive task must be in 'list of text' format",
                    received_value=tags,
                )
            if len(tags) > ALLOWED_TAGS_COUNT:
                raise _FlyteValueException(
                    len(tags),
                    "number of tags must be less than {}".format(ALLOWED_TAGS_COUNT),
                )
            if not all(len(tag) for tag in tags):
                raise _FlyteValueException(
                    tags,
                    "length of a tag must be less than {} chars".format(MAX_TAG_LENGTH),
                )

    @staticmethod
    def _validate_queries(queries_from_task):
        for query_from_task in queries_from_task or []:
            if not isinstance(query_from_task, (str, _six.text_type)):
                raise _FlyteTypeException(
                    type(query_from_task),
                    {str, _six.text_type},
                    additional_msg="All queries returned from a Hive task must be in text format.",
                    received_value=query_from_task,
                )

    def _produce_dynamic_job_spec(self, context, inputs):
        """
        Runs user code and and produces future task nodes to run sub-tasks.
        :param context:
        :param flytekit.models.literals.LiteralMap literal_map inputs:
        :rtype: flytekit.models.dynamic_job.DynamicJobSpec
        """
        inputs_dict = _type_helpers.unpack_literal_map_to_sdk_python_std(
            inputs,
            {k: _type_helpers.get_sdk_type_from_literal_type(v.type) for k, v in _six.iteritems(self.interface.inputs)},
        )
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
        output_bindings = [
            _literal_models.Binding(
                var=name,
                binding=_interface.BindingData.from_python_std(b.sdk_type.to_flyte_literal_type(), b.value),
            )
            for name, b in _six.iteritems(outputs_dict)
        ]

        i = 0
        for quboleHiveJob in generated_queries:
            hive_job_node = _create_hive_job_node("HiveQuery_{}".format(i), quboleHiveJob.to_flyte_idl(), self.metadata)
            nodes.append(hive_job_node)
            tasks.append(hive_job_node.executable_sdk_object)
            i += 1

        dynamic_job_spec = _dynamic_job.DynamicJobSpec(
            min_successes=len(nodes),
            tasks=tasks,
            nodes=nodes,
            outputs=output_bindings,
            subworkflows=[],
        )

        return dynamic_job_spec

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
                    literals={binding.var: binding.binding.to_literal_model() for binding in spec.outputs}
                )
            }
        else:
            generated_files.update({_constants.FUTURES_FILE_NAME: spec})

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
        sdk_task=SdkHiveJob(hive_job, metadata),
    )


class SdkHiveJob(_base_task.SdkTask):
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
        super(SdkHiveJob, self).__init__(
            _constants.SdkTaskType.HIVE_JOB,
            metadata,
            # Individual hive tasks never take anything, or return anything. They just run a query that's already
            # got the location set.
            _interface_model.TypedInterface({}, {}),
            _MessageToDict(hive_job),
        )
