"""User-facing tasks for SageMaker training jobs."""

from dataclasses import dataclass
from typing import Any, Dict, Optional, Type, Union

from flytekitplugins.awssagemaker_inference.boto3_task import BotoConfig, BotoTask
from flytekitplugins.awssagemaker_inference.pythonic_base import (
    PythonicJobConfig,
    PythonicSageMakerJobTask,
)

from flytekit import ImageSpec, kwtypes
from flytekit.configuration import SerializationSettings
from flytekit.core.base_task import PythonTask
from flytekit.core.interface import Interface
from flytekit.extend import TaskPlugins
from flytekit.extend.backend.base_connector import AsyncConnectorExecutorMixin
from flytekit.image_spec.image_spec import ImageBuildEngine


@dataclass
class SageMakerTraining(PythonicJobConfig):
    """Pythonic-mode config for a SageMaker training job.

    Use as ``@task(task_config=SageMakerTraining(...), container_image=...)`` to
    run the decorated Python function inside a SageMaker training job. Inherits
    all fields from
    :class:`~flytekitplugins.awssagemaker_inference.pythonic_base.PythonicJobConfig`.

    :param output_s3_path: Optional ``OutputDataConfig.S3OutputPath`` for the
        SageMaker model tar. Pythonic mode returns results via Flyte outputs, so
        this is rarely needed; when unset the connector points it at the Flyte
        output prefix (the resulting ``model.tar.gz`` is harmless and unused).
    :param vpc_config: Optional boto3 ``VpcConfig`` request shape containing
        ``Subnets`` and ``SecurityGroupIds``.
    """

    output_s3_path: Optional[str] = None
    vpc_config: Optional[Dict[str, Any]] = None


class SageMakerTrainingTask(PythonicSageMakerJobTask):
    """Pythonic-mode SageMaker training task (runs a ``@task`` function in a training job)."""

    _TASK_TYPE = "sagemaker-training-task"


TaskPlugins.register_pythontask_plugin(SageMakerTraining, SageMakerTrainingTask)


class SageMakerTrainingJobTask(AsyncConnectorExecutorMixin, PythonTask):
    """Run a SageMaker training job and emit its model artefact URI plus final metrics.

    Outputs a single ``result: dict`` literal containing ``TrainingJobArn``,
    ``TrainingJobName``, ``ModelArtifacts.S3ModelArtifacts`` (the S3 URI of the
    trained ``model.tar.gz`` — feed this into ``SageMakerModelTask`` to deploy),
    ``OutputDataConfig.S3OutputPath``, ``FinalMetricDataList`` (last value of every
    metric defined in ``AlgorithmSpecification.MetricDefinitions``),
    ``BillableTimeInSeconds`` and ``TrainingTimeInSeconds``.

    ``name`` identifies the Flyte task. ``config`` is the boto3
    ``create_training_job`` request and may contain ``{inputs.X}``,
    ``{images.X}``, and ``{idempotence_token}`` placeholders. ``region`` selects
    the AWS region. ``images`` maps image placeholders to image URIs or
    ``ImageSpec`` objects, and ``inputs`` maps input placeholders to Flyte types.
    """

    _TASK_TYPE = "sagemaker-training-job"

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


class SageMakerStopTrainingJobTask(BotoTask):
    """Sync helper task that stops a running SageMaker training job by name."""

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
                method="stop_training_job",
                config=config,
                region=region,
            ),
            inputs=inputs,
            **kwargs,
        )


class SageMakerDescribeTrainingJobTask(BotoTask):
    """Sync helper task that returns the full ``describe_training_job`` response."""

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
                method="describe_training_job",
                config=config,
                region=region,
            ),
            inputs=inputs,
            **kwargs,
        )
