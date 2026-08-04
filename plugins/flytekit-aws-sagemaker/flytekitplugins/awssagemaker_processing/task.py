"""User-facing tasks for SageMaker processing jobs."""

from typing import Any, Dict, Optional, Type, Union

from flytekitplugins.awssagemaker_inference.boto3_task import BotoConfig, BotoTask

from flytekit import ImageSpec, kwtypes
from flytekit.configuration import SerializationSettings
from flytekit.core.base_task import PythonTask
from flytekit.core.interface import Interface
from flytekit.extend.backend.base_connector import AsyncConnectorExecutorMixin
from flytekit.image_spec.image_spec import ImageBuildEngine


class SageMakerProcessingJobTask(AsyncConnectorExecutorMixin, PythonTask):
    """Run a SageMaker processing job and emit its output S3 URIs.

    Processing jobs cover data pre/post-processing, feature engineering, model
    evaluation, and SageMaker Clarify (bias / explainability) — the steps that
    bookend training. The container image lives at ``AppSpecification.ImageUri``;
    inputs are commonly S3-resident, while outputs can target S3 or SageMaker
    Feature Store through ``ProcessingOutputConfig``.

    Outputs a single ``result: dict`` literal containing ``ProcessingJobArn``,
    ``ProcessingJobName``, ``Outputs`` (a list containing ``OutputName`` plus
    either ``S3Uri`` for S3 destinations or ``FeatureGroupName`` for Feature
    Store destinations), and ``ExitMessage``.

    ``name`` identifies the Flyte task. ``config`` is the boto3
    ``create_processing_job`` request and may contain ``{inputs.X}``,
    ``{images.X}``, and ``{idempotence_token}`` placeholders. ``region`` selects
    the AWS region. ``images`` maps image placeholders to image URIs or
    ``ImageSpec`` objects, and ``inputs`` maps input placeholders to Flyte types.
    """

    _TASK_TYPE = "sagemaker-processing-job"

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


class SageMakerStopProcessingJobTask(BotoTask):
    """Sync helper task that stops a running SageMaker processing job by name."""

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
                method="stop_processing_job",
                config=config,
                region=region,
            ),
            inputs=inputs,
            **kwargs,
        )


class SageMakerDescribeProcessingJobTask(BotoTask):
    """Sync helper task that returns the full ``describe_processing_job`` response."""

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
                method="describe_processing_job",
                config=config,
                region=region,
            ),
            inputs=inputs,
            **kwargs,
        )
