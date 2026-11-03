"""User-facing tasks for SageMaker hyperparameter-tuning jobs."""

from typing import Any, Dict, Optional, Type, Union

from flytekitplugins.awssagemaker_inference.boto3_task import BotoConfig, BotoTask

from flytekit import ImageSpec, kwtypes
from flytekit.configuration import SerializationSettings
from flytekit.core.base_task import PythonTask
from flytekit.core.interface import Interface
from flytekit.extend.backend.base_connector import AsyncConnectorExecutorMixin
from flytekit.image_spec.image_spec import ImageBuildEngine


class SageMakerHyperParameterTuningJobTask(AsyncConnectorExecutorMixin, PythonTask):
    """Run a SageMaker hyperparameter-tuning job and emit the best trial's artefacts.

    Outputs a single ``result: dict`` literal containing:

    - ``HyperParameterTuningJobArn``, ``HyperParameterTuningJobName``
    - ``BestTrainingJob`` — the trial SageMaker picked: ``TrainingJobName``,
      ``TrainingJobArn``, ``TunedHyperParameters``,
      ``FinalHyperParameterTuningJobObjectiveMetric.{MetricName, Value}``,
      ``ObjectiveStatus``, plus the trial's ``ModelArtifacts.S3ModelArtifacts``
      resolved via a follow-up ``describe_training_job`` call (so this output
      chains directly into ``SageMakerModelTask`` the same way the plain
      ``SageMakerTrainingJobTask`` does)
    - ``ModelArtifacts.S3ModelArtifacts`` — top-level convenience copy of the
      best trial's model URI so workflows can consume it symmetrically with
      training-job results
    - ``TrainingJobStatusCounters`` — how many trials Completed / InProgress /
      RetryableError / NonRetryableError / Stopped
    - ``ObjectiveStatusCounters`` — Succeeded / Pending / Failed at the
      objective-metric layer (a trial can Complete but Fail to emit the
      objective metric — that lands in ``ObjectiveStatusCounters.Failed``)

    ``name`` identifies the Flyte task. ``config`` is the boto3
    ``create_hyper_parameter_tuning_job`` request and may contain
    ``{inputs.X}``, ``{images.X}``, and ``{idempotence_token}`` placeholders.
    ``region`` selects the AWS region. ``images`` maps trial-image placeholders
    to image URIs or ``ImageSpec`` objects, and ``inputs`` maps input
    placeholders to Flyte types.
    """

    _TASK_TYPE = "sagemaker-hyperparameter-tuning-job"

    def __init__(
        self,
        name: str,
        config: Dict[str, Any],
        region: Optional[str] = None,
        images: Optional[Dict[str, Union[str, ImageSpec]]] = None,
        inputs: Optional[Dict[str, Type]] = None,
        **kwargs,
    ):
        super().__init__(
            name=name,
            task_type=self._TASK_TYPE,
            interface=Interface(inputs=inputs, outputs=kwtypes(result=dict)),
            **kwargs,
        )
        self._config = config
        self._region = region
        self._images = images

    def get_custom(self, settings: SerializationSettings) -> Dict[str, Any]:
        images = self._images
        if images is not None:
            for key, image in images.items():
                if isinstance(image, ImageSpec):
                    ImageBuildEngine.build(image)
                    images[key] = image.image_name()
        return {"config": self._config, "region": self._region, "images": images}


class SageMakerStopHyperParameterTuningJobTask(BotoTask):
    """Sync helper task that stops a running SageMaker hyperparameter-tuning job by name."""

    def __init__(
        self,
        name: str,
        config: Dict[str, Any],
        region: Optional[str] = None,
        inputs: Optional[Dict[str, Type]] = None,
        **kwargs,
    ):
        super().__init__(
            name=name,
            task_config=BotoConfig(
                service="sagemaker",
                method="stop_hyper_parameter_tuning_job",
                config=config,
                region=region,
            ),
            inputs=inputs,
            **kwargs,
        )


class SageMakerDescribeHyperParameterTuningJobTask(BotoTask):
    """Sync helper task that returns the full ``describe_hyper_parameter_tuning_job`` response."""

    def __init__(
        self,
        name: str,
        config: Dict[str, Any],
        region: Optional[str] = None,
        inputs: Optional[Dict[str, Type]] = None,
        **kwargs,
    ):
        super().__init__(
            name=name,
            task_config=BotoConfig(
                service="sagemaker",
                method="describe_hyper_parameter_tuning_job",
                config=config,
                region=region,
            ),
            inputs=inputs,
            **kwargs,
        )
