from __future__ import absolute_import

import datetime as _datetime
import os as _os
import json as _json
import six as _six

from google.protobuf import text_format as _text_format
from pyspark import SparkConf, SparkContext
from flytekit.sdk.types import Types as _Types
from flytekit.common.types import helpers as _type_helpers, primitives as _p
from flytekit.common import constants as _constants, sdk_bases as _sdk_bases
from flytekit.common.exceptions import scopes as _exception_scopes
from flytekit.common.tasks import output as _task_output, task as _base_tasks
from flytekit.models import literals as _literal_models

type_map = {
    int: _p.Integer,
    bool: _p.Boolean,
    float: _p.Float,
    str: _p.String,
    _datetime.datetime: _p.Datetime,
    _datetime.timedelta: _p.Timedelta,
}

OUTPUT_NOTEBOOK = 'output_notebook'


# TODO: Move to spark  task
def get_spark_context(spark_conf):
    """
       outputs: SparkContext
       Returns appropriate SparkContext based on whether invoked via a Notebook or a Flyte workflow.
    """
    # We run in cluster-mode in Flyte.
    # Ref https://github.com/lyft/flyteplugins/blob/master/go/tasks/v1/flytek8s/k8s_resource_adds.go#L46
    if "FLYTE_INTERNAL_EXECUTION_ID" in _os.environ:
        return SparkContext()

    # Add system spark-conf for local/notebook based execution.
    spark_conf.add(("spark.master", "local"))
    conf = SparkConf().setAll(spark_conf)
    return SparkContext(conf=conf)


def python_notebook(
        notebook_path='',
        inputs={},
        outputs={},
        cache_version='',
        retries=0,
        deprecated='',
        storage_request=None,
        cpu_request=None,
        gpu_request=None,
        memory_request=None,
        storage_limit=None,
        cpu_limit=None,
        gpu_limit=None,
        memory_limit=None,
        cache=False,
        timeout=None,
        environment=None,
        cls=None,

):
    """
    Decorator to create a Python Notebook Task definition.  This task will run as a single unit of work on the platform.

    :rtype: flytekit.common.tasks.sdk_runnable.SdkNotebookTask
    """
    return SdkNotebookTask(
            notebook_path=notebook_path,
            inputs=inputs,
            outputs=outputs,
            task_type=_constants.SdkTaskType.PYTHON_TASK,
            discovery_version=cache_version,
            retries=retries,
            deprecated=deprecated,
            storage_request=storage_request,
            cpu_request=cpu_request,
            gpu_request=gpu_request,
            memory_request=memory_request,
            storage_limit=storage_limit,
            cpu_limit=cpu_limit,
            gpu_limit=gpu_limit,
            memory_limit=memory_limit,
            discoverable=cache,
            timeout=timeout or _datetime.timedelta(seconds=0),
            environment=environment,
            custom={})


class SdkNotebookTask(
        _six.with_metaclass(_sdk_bases.ExtendedSdkType, _base_tasks.SdkTask)):

    @_exception_scopes.system_entry_point
    def execute(self, context, inputs):
        """
        :param flytekit.engines.common.EngineContext context:
        :param flytekit.models.literals.LiteralMap inputs:
        :rtype: dict[Text, flytekit.models.common.FlyteIdlEntity]
        :returns: This function must return a dictionary mapping 'filenames' to Flyte Interface Entities.  These
            entities will be used by the engine to pass data from node to node, populate metadata, etc. etc..  Each
            engine will have different behavior.  For instance, the Flyte engine will upload the entities to a remote
            working directory (with the names provided), which will in turn allow Flyte Propeller to push along the
            workflow.  Where as local engine will merely feed the outputs directly into the next node.
        """
        inputs_dict = _type_helpers.unpack_literal_map_to_sdk_python_std(inputs, {
            k: _type_helpers.get_sdk_type_from_literal_type(v.type) for k, v in _six.iteritems(self.interface.inputs)
        })

        input_notebook_path = self._notebook_path
        # Execute Notebook via Papermill.
        output_notebook_path = input_notebook_path + '.out'
        _pm.execute_notebook(
            input_notebook_path,
            output_notebook_path,
            parameters=inputs_dict
        )

        # Parse Outputs from Notebook.
        outputs = None
        with open(output_notebook_path) as json_file:
            data = _json.load(json_file)
            for p in data['cells']:
                meta = p['metadata']
                if "outputs" in meta["tags"]:
                    outputs = ' '.join(p['outputs'][0]['data']['text/plain'])

        if outputs is not None:
            dict = _literal_models._literals_pb2.LiteralMap()
            _text_format.Parse(outputs, dict)

        # Add output_notebook as an output to the task.
        output_notebook = _task_output.OutputReference(
            _type_helpers.get_sdk_type_from_literal_type(_Types.Blob.to_flyte_literal_type()))
        output_notebook.set(output_notebook_path)

        output_literal_map = _literal_models.LiteralMap.from_flyte_idl(dict)
        output_literal_map.literals[OUTPUT_NOTEBOOK] = output_notebook.sdk_value

        return {
            _constants.OUTPUT_FILE_NAME: output_literal_map
        }
