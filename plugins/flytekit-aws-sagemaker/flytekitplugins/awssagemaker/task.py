from dataclasses import dataclass
from typing import Any, Optional, Type, Union

from flytekit import ImageSpec, kwtypes
from flytekit.configuration import DefaultImages, SerializationSettings
from flytekit.core.base_task import PythonTask
from flytekit.core.interface import Interface
from flytekit.extend.backend.base_agent import AsyncAgentExecutorMixin

from .boto3_task import BotoConfig, BotoTask


class SageMakerModelTask(BotoTask):
    def __init__(
        self,
        name: str,
        config: dict[str, Any],
        region: Optional[str],
        inputs: Optional[dict[str, Type]] = None,
        container_image: Optional[Union[str, ImageSpec]] = None,
        **kwargs,
    ):
        """
        Creates a SageMaker model.

        :param name: The name of the task.
        :param config: The configuration to be provided to the boto3 API call.
        :param region: The region for the boto3 client.
        :param inputs: The input literal map to be used for updating the configuration.
        :param container_image: The path where inference code is stored.
                                This can be either in Amazon EC2 Container Registry or in a Docker registry
                                that is accessible from the same VPC that you configure for your endpoint.
        """
        super(SageMakerModelTask, self).__init__(
            name=name,
            task_config=BotoConfig(service="sagemaker", method="create_model", config=config, region=region),
            inputs=inputs,
            outputs=kwtypes(result=dict),
            container_image=container_image,
            **kwargs,
        )


class SageMakerEndpointConfigTask(BotoTask):
    def __init__(
        self,
        name: str,
        config: dict[str, Any],
        region: Optional[str],
        inputs: Optional[dict[str, Type]] = None,
        **kwargs,
    ):
        """
        Creates a SageMaker endpoint configuration.

        :param name: The name of the task.
        :param config: The configuration to be provided to the boto3 API call.
        :param region: The region for the boto3 client.
        :param inputs: The input literal map to be used for updating the configuration.
        """
        super(SageMakerEndpointConfigTask, self).__init__(
            name=name,
            task_config=BotoConfig(
                service="sagemaker",
                method="create_endpoint_config",
                config=config,
                region=region,
            ),
            inputs=inputs,
            outputs=kwtypes(result=dict),
            container_image=DefaultImages.default_image(),
            **kwargs,
        )


@dataclass
class SageMakerEndpointMetadata(object):
    config: dict[str, Any]
    region: str


class SageMakerEndpointTask(AsyncAgentExecutorMixin, PythonTask[SageMakerEndpointMetadata]):
    _TASK_TYPE = "sagemaker-endpoint"

    def __init__(
        self,
        name: str,
        config: dict[str, Any],
        region: Optional[str],
        inputs: Optional[dict[str, Type]] = None,
        **kwargs,
    ):
        """
        Creates a SageMaker endpoint.

        :param name: The name of the task.
        :param config: The configuration to be provided to the boto3 API call.
        :param region: The region for the boto3 client.
        :param inputs: The input literal map to be used for updating the configuration.
        """
        super().__init__(
            name=name,
            task_config=SageMakerEndpointMetadata(
                config=config,
                region=region,
            ),
            task_type=self._TASK_TYPE,
            interface=Interface(inputs=inputs, outputs=kwtypes(result=str)),
            **kwargs,
        )

    def get_custom(self, settings: SerializationSettings) -> dict[str, Any]:
        return {"config": self.task_config.config, "region": self.task_config.region}


class SageMakerDeleteEndpointTask(BotoTask):
    def __init__(
        self,
        name: str,
        config: dict[str, Any],
        region: Optional[str],
        inputs: Optional[dict[str, Type]] = None,
        **kwargs,
    ):
        """
        Deletes a SageMaker endpoint.

        :param name: The name of the task.
        :param config: The configuration to be provided to the boto3 API call.
        :param region: The region for the boto3 client.
        :param inputs: The input literal map to be used for updating the configuration.
        """
        super(SageMakerDeleteEndpointTask, self).__init__(
            name=name,
            task_config=BotoConfig(
                service="sagemaker",
                method="delete_endpoint",
                config=config,
                region=region,
            ),
            inputs=inputs,
            container_image=DefaultImages.default_image(),
            **kwargs,
        )


class SageMakerDeleteEndpointConfigTask(BotoTask):
    def __init__(
        self,
        name: str,
        config: dict[str, Any],
        region: Optional[str],
        inputs: Optional[dict[str, Type]] = None,
        **kwargs,
    ):
        """
        Deletes a SageMaker endpoint config.

        :param name: The name of the task.
        :param config: The configuration to be provided to the boto3 API call.
        :param region: The region for the boto3 client.
        :param inputs: The input literal map to be used for updating the configuration.
        """
        super(SageMakerDeleteEndpointConfigTask, self).__init__(
            name=name,
            task_config=BotoConfig(
                service="sagemaker",
                method="delete_endpoint_config",
                config=config,
                region=region,
            ),
            inputs=inputs,
            container_image=DefaultImages.default_image(),
            **kwargs,
        )


class SageMakerDeleteModelTask(BotoTask):
    def __init__(
        self,
        name: str,
        config: dict[str, Any],
        region: Optional[str],
        inputs: Optional[dict[str, Type]] = None,
        **kwargs,
    ):
        """
        Deletes a SageMaker model.

        :param name: The name of the task.
        :param config: The configuration to be provided to the boto3 API call.
        :param region: The region for the boto3 client.
        :param inputs: The input literal map to be used for updating the configuration.
        """
        super(SageMakerDeleteModelTask, self).__init__(
            name=name,
            task_config=BotoConfig(
                service="sagemaker",
                method="delete_model",
                config=config,
                region=region,
            ),
            inputs=inputs,
            container_image=DefaultImages.default_image(),
            **kwargs,
        )


class SageMakerInvokeEndpointTask(BotoTask):
    def __init__(
        self,
        name: str,
        config: dict[str, Any],
        region: Optional[str],
        inputs: Optional[dict[str, Type]] = None,
        **kwargs,
    ):
        """
        Invokes a SageMaker endpoint.

        :param name: The name of the task.
        :param config: The configuration to be provided to the boto3 API call.
        :param region: The region for the boto3 client.
        :param inputs: The input literal map to be used for updating the configuration.
        """
        super(SageMakerInvokeEndpointTask, self).__init__(
            name=name,
            task_config=BotoConfig(
                service="sagemaker-runtime",
                method="invoke_endpoint_async",
                config=config,
                region=region,
            ),
            inputs=inputs,
            outputs=kwtypes(result=dict),
            container_image=DefaultImages.default_image(),
            **kwargs,
        )
