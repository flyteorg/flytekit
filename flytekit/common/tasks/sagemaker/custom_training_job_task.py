import logging as _logging
import typing as _typing

import six as _six
from google.protobuf.json_format import MessageToDict

from flytekit.common.constants import SdkTaskType
from flytekit.common.core.identifier import WorkflowExecutionIdentifier
from flytekit.common.exceptions import scopes as _exception_scopes
from flytekit.common.tasks import sdk_runnable as _sdk_runnable
from flytekit.common.tasks.sagemaker import distributed_training as _sm_distribution
from flytekit.common.tasks.sagemaker.distributed_training import DefaultOutputPersistPredicate
from flytekit.models.sagemaker import training_job as _training_job_models


class CustomTrainingJobTask(_sdk_runnable.SdkRunnableTask):
    """
    CustomTrainJobTask defines a python task that can run on SageMaker bring your own container.

    """

    def __init__(
        self,
        task_function,
        cache_version,
        retries,
        deprecated,
        storage_request,
        cpu_request,
        gpu_request,
        memory_request,
        storage_limit,
        cpu_limit,
        gpu_limit,
        memory_limit,
        cache,
        timeout,
        environment,
        algorithm_specification: _training_job_models.AlgorithmSpecification,
        training_job_resource_config: _training_job_models.TrainingJobResourceConfig,
        output_persist_predicate: _typing.Callable = DefaultOutputPersistPredicate(),
    ):
        """
        :param task_function: Function container user code.  This will be executed via the SDK's engine.
        :param Text cache_version: string describing the version for task discovery purposes
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
        :param bool cache:
        :param datetime.timedelta timeout:
        :param dict[Text, Text] environment:
        :param _training_job_models.AlgorithmSpecification algorithm_specification:
        :param _training_job_models.TrainingJobResourceConfig training_job_resource_config:
        :param _typing.Callable output_persist_predicate:
        """

        self._output_persist_predicate = output_persist_predicate

        # Use the training job model as a measure of type checking
        self._training_job_model = _training_job_models.TrainingJob(
            algorithm_specification=algorithm_specification, training_job_resource_config=training_job_resource_config
        )

        super().__init__(
            task_function=task_function,
            task_type=SdkTaskType.SAGEMAKER_CUSTOM_TRAINING_JOB_TASK,
            discovery_version=cache_version,
            retries=retries,
            interruptible=False,
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
            timeout=timeout,
            environment=environment,
            custom=MessageToDict(self._training_job_model.to_flyte_idl()),
        )

    @property
    def output_persist_predicate(self):
        return self._output_persist_predicate

    @property
    def training_job_model(self) -> _training_job_models.TrainingJob:
        return self._training_job_model

    def _is_distributed(self):
        return (
            self.training_job_model.training_job_resource_config
            and self.training_job_model.training_job_resource_config.instance_count > 1
        )

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
        dist_training_task_specific_engine_context = _sm_distribution.DistributedTrainingEngineContext(
            execution_date=context.execution_date,
            tmp_dir=context.working_directory,
            stats=context.stats,
            execution_id=context.execution_id,
            logging=context.logging,
            raw_output_data_prefix=context.raw_output_data_prefix,
            distributed_training_context=_sm_distribution.get_sagemaker_distributed_training_context_from_env(),
        )

        ret = super().execute(dist_training_task_specific_engine_context, inputs)

        if (
            self._is_distributed()
            and self._output_persist_predicate
            and self.output_persist_predicate(dist_training_task_specific_engine_context.distributed_training_context)
            is True
        ):
            return ret
        else:
            _logging.info(
                "Output_persist_predicate() returns False for this instance. "
                "The output of this task will not be persisted"
            )
            return {}

    def _execute_user_code(self, context, inputs):
        """
        :param flytekit.engines.common.tasks.sagemaker.distribution.DistributedTrainingEngineContext context:
        :param dict[Text, T] inputs: This variable is a bit of a misnomer, since it's both inputs and outputs. The
            dictionary passed here will be passed to the user-defined function, and will have values that are a
            variety of types.  The T's here are Python std values for inputs.  If there isn't a native Python type for
            something (like Schema or Blob), they are the Flyte classes.  For outputs they are OutputReferences.
            (Note that these are not the same OutputReferences as in BindingData's)
        :rtype: Any: the returned object from user code.
        :returns: This function must return a dictionary mapping 'filenames' to Flyte Interface Entities.  These
            entities will be used by the engine to pass data from node to node, populate metadata, etc. etc..  Each
            engine will have different behavior.  For instance, the Flyte engine will upload the entities to a remote
            working directory (with the names provided), which will in turn allow Flyte Propeller to push along the
            workflow.  Where as local engine will merely feed the outputs directly into the next node.
        """

        return _exception_scopes.user_entry_point(self.task_function)(
            _sm_distribution.DistributedTrainingExecutionParam(
                execution_date=context.execution_date,
                # TODO: it might be better to consider passing the full struct
                execution_id=_six.text_type(WorkflowExecutionIdentifier.promote_from_model(context.execution_id)),
                stats=context.stats,
                logging=context.logging,
                tmp_dir=context.working_directory,
                distributed_training_context=context.distributed_training_context,
            ),
            **inputs
        )
