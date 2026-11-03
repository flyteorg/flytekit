"""User-facing tasks for SageMaker batch-transform jobs."""

from typing import Any, Dict, Optional, Type

from flytekitplugins.awssagemaker_inference.boto3_task import BotoConfig, BotoTask

from flytekit import kwtypes
from flytekit.configuration import SerializationSettings
from flytekit.core.base_task import PythonTask
from flytekit.core.interface import Interface
from flytekit.extend.backend.base_connector import AsyncConnectorExecutorMixin


class SageMakerTransformJobTask(AsyncConnectorExecutorMixin, PythonTask):
    """Run a SageMaker batch-transform job and emit the predictions ``S3OutputPath``.

    Outputs a single ``result: dict`` literal containing ``TransformJobArn``,
    ``TransformJobName``, ``ModelName``, ``TransformOutput.S3OutputPath`` (the S3
    prefix where SageMaker wrote one ``<input>.out`` per input object — feed this
    into a downstream Flyte task to consume the predictions), ``TransformStartTime``
    and ``TransformEndTime``.

    Set ``DataProcessing.JoinSource: "Input"`` in the config for tabular predictive
    workloads so each output line carries the original input fields alongside the
    prediction (otherwise rows have no key to join back).

    ``name`` identifies the Flyte task. ``config`` is the boto3
    ``create_transform_job`` request and may contain ``{inputs.X}`` and
    ``{idempotence_token}`` placeholders. ``region`` selects the AWS region, and
    ``inputs`` maps input placeholders to Flyte types.
    """

    _TASK_TYPE = "sagemaker-transform-job"

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
            task_type=self._TASK_TYPE,
            interface=Interface(inputs=inputs, outputs=kwtypes(result=dict)),
            **kwargs,
        )
        self._config = config
        self._region = region

    def get_custom(self, settings: SerializationSettings) -> Dict[str, Any]:
        return {"config": self._config, "region": self._region}


class SageMakerStopTransformJobTask(BotoTask):
    """Sync helper task that stops a running SageMaker transform job by name."""

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
                method="stop_transform_job",
                config=config,
                region=region,
            ),
            inputs=inputs,
            **kwargs,
        )


class SageMakerDescribeTransformJobTask(BotoTask):
    """Sync helper task that returns the full ``describe_transform_job`` response."""

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
                method="describe_transform_job",
                config=config,
                region=region,
            ),
            inputs=inputs,
            **kwargs,
        )
