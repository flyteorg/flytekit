from __future__ import absolute_import

try:
    from inspect import getfullargspec as _getargspec
except ImportError:
    from inspect import getargspec as _getargspec

import six as _six
from flytekit.common import constants as _constants
from flytekit.common.exceptions import scopes as _exception_scopes
from flytekit.common.tasks import output as _task_output, sdk_runnable as _sdk_runnable
from flytekit.common.types import helpers as _type_helpers
from flytekit.models import literals as _literal_models, task as _task_models
from google.protobuf.json_format import MessageToDict as _MessageToDict


class SdkRunnablePytorchkContainer(_sdk_runnable.SdkRunnableContainer):

    @property
    def args(self):
        """
        Override args to remove the injection of command prefixes
        :rtype: list[Text]
        """
        return self._args

class SdkPyTorchTask(_sdk_runnable.SdkRunnableTask):
    def __init__(
            self,
            task_function,
            task_type,
            discovery_version,
            retries,
            interruptible,
            deprecated,
            discoverable,
            timeout,
            workers_count,
            instance_storage_request,
            instance_cpu_request,
            instance_gpu_request,
            instance_memory_request,
            instance_storage_limit,
            instance_cpu_limit,
            instance_gpu_limit,
            instance_memory_limit,
            environment
    ):
        pytorch_job = _task_models.PyTorchJob(
            workers_count=workers_count
        ).to_flyte_idl()
        super(SdkPyTorchTask, self).__init__(
            task_function=task_function,
            task_type=task_type,
            discovery_version=discovery_version,
            retries=retries,
            interruptible=interruptible,
            deprecated=deprecated,
            storage_request=instance_storage_request,
            cpu_request=instance_cpu_request,
            gpu_request=instance_gpu_request,
            memory_request=instance_memory_request,
            storage_limit=instance_storage_limit,
            cpu_limit=instance_cpu_limit,
            gpu_limit=instance_gpu_limit,
            memory_limit=instance_memory_limit,
            discoverable=discoverable,
            timeout=timeout,
            environment=environment,
            custom=_MessageToDict(pytorch_job)
        )

    @_exception_scopes.system_entry_point
    def execute(self, context, inputs):
        """
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
        outputs_dict = {
            name: _task_output.OutputReference(_type_helpers.get_sdk_type_from_literal_type(variable.type))
            for name, variable in _six.iteritems(self.interface.outputs)
        }

        inputs_dict.update(outputs_dict)

        _exception_scopes.user_entry_point(self.task_function)(
            _sdk_runnable.ExecutionParameters(
                execution_date=context.execution_date,
                execution_id=context.execution_id,
                stats=context.stats,
                logging=context.logging,
                tmp_dir=context.working_directory
            ),
            **inputs_dict
        )
        return {
            _constants.OUTPUT_FILE_NAME: _literal_models.LiteralMap(
                literals={k: v.sdk_value for k, v in _six.iteritems(outputs_dict)}
            )
        }

    def _get_container_definition(
            self,
            **kwargs
    ):
        """
        :rtype: SdkRunnableSparkContainer
        """
        return super(SdkPyTorchTask, self)._get_container_definition(cls=SdkRunnablePytorchkContainer, **kwargs)

    def _get_kwarg_inputs(self):
        # Trim off first parameter as it is reserved for workflow_parameters
        return set(_getargspec(self.task_function).args[1:])