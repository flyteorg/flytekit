from dataclasses import dataclass
from typing import Any, Optional, Type, Union


from flytekit.configuration import SerializationSettings, DefaultImages
from flytekit.core.base_task import PythonTask
from flytekit.core.interface import Interface
from flytekit.extend.backend.base_agent import AsyncAgentExecutorMixin
from .boto3_task import BotoTask, BotoConfig
from flytekit import ImageSpec


class SagemakerModelTask(BotoTask):
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
        Creates a Sagemaker model.

        :param name: The name of the task.
        :param config: The configuration to be provided to the boto3 API call.
        :param region: The region for the boto3 client.
        :param inputs: The input literal map to be used for updating the configuration.
        :param container_image: The path where inference code is stored.
                                This can be either in Amazon EC2 Container Registry or in a Docker registry
                                that is accessible from the same VPC that you configure for your endpoint.
        """
        super(SagemakerModelTask, self).__init__(
            name=name,
            task_config=BotoConfig(service="sagemaker", method="create_model", config=config, region=region),
            inputs=inputs,
            output_type=dict[str, str],
            container_image=container_image,
            **kwargs,
        )


class SagemakerEndpointConfigTask(BotoTask):
    def __init__(
        self,
        name: str,
        config: dict[str, Any],
        region: Optional[str],
        inputs: Optional[dict[str, Type]] = None,
        **kwargs,
    ):
        """
        Creates a Sagemaker endpoint configuration.

        :param name: The name of the task.
        :param config: The configuration to be provided to the boto3 API call.
        :param region: The region for the boto3 client.
        :param inputs: The input literal map to be used for updating the configuration.
        """
        super(SagemakerEndpointConfigTask, self).__init__(
            name=name,
            task_config=BotoConfig(
                service="sagemaker",
                method="create_endpoint_config",
                config=config,
                region=region,
            ),
            inputs=inputs,
            output_type=dict[str, str],
            container_image=DefaultImages.default_image(),
            **kwargs,
        )


@dataclass
class SagemakerEndpointMetadata(object):
    config: dict[str, Any]
    region: str


class SagemakerEndpointTask(AsyncAgentExecutorMixin, PythonTask[SagemakerEndpointMetadata]):
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
        Creates a Sagemaker endpoint.

        :param name: The name of the task.
        :param config: The configuration to be provided to the boto3 API call.
        :param region: The region for the boto3 client.
        :param inputs: The input literal map to be used for updating the configuration.
        """
        super().__init__(
            name=name,
            task_config=SagemakerEndpointMetadata(
                config=config,
                region=region,
            ),
            task_type=self._TASK_TYPE,
            interface=Interface(inputs=inputs, outputs={"result": str}),
            **kwargs,
        )

    def get_custom(self, settings: SerializationSettings) -> dict[str, Any]:
        return {"config": self.task_config.config, "region": self.task_config.region}


class SagemakerDeleteEndpointTask(BotoTask):
    def __init__(
        self,
        name: str,
        config: dict[str, Any],
        region: Optional[str],
        inputs: Optional[dict[str, Type]] = None,
        **kwargs,
    ):
        """
        Deletes a Sagemaker endpoint.

        :param name: The name of the task.
        :param config: The configuration to be provided to the boto3 API call.
        :param region: The region for the boto3 client.
        :param inputs: The input literal map to be used for updating the configuration.
        """
        super(SagemakerDeleteEndpointTask, self).__init__(
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


class SagemakerDeleteEndpointConfigTask(BotoTask):
    def __init__(
        self,
        name: str,
        config: dict[str, Any],
        region: Optional[str],
        inputs: Optional[dict[str, Type]] = None,
        **kwargs,
    ):
        """
        Deletes a Sagemaker endpoint config.

        :param name: The name of the task.
        :param config: The configuration to be provided to the boto3 API call.
        :param region: The region for the boto3 client.
        :param inputs: The input literal map to be used for updating the configuration.
        """
        super(SagemakerDeleteEndpointConfigTask, self).__init__(
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


class SagemakerDeleteModelTask(BotoTask):
    def __init__(
        self,
        name: str,
        config: dict[str, Any],
        region: Optional[str],
        inputs: Optional[dict[str, Type]] = None,
        **kwargs,
    ):
        """
        Deletes a Sagemaker model.

        :param name: The name of the task.
        :param config: The configuration to be provided to the boto3 API call.
        :param region: The region for the boto3 client.
        :param inputs: The input literal map to be used for updating the configuration.
        """
        super(SagemakerDeleteModelTask, self).__init__(
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


class SagemakerInvokeEndpointTask(BotoTask):
    def __init__(
        self,
        name: str,
        config: dict[str, Any],
        region: Optional[str],
        output_type: Optional[Type] = None,
        inputs: Optional[dict[str, Type]] = None,
        **kwargs,
    ):
        """
        Invokes a Sagemaker endpoint.

        :param name: The name of the task.
        :param config: The configuration to be provided to the boto3 API call.
        :param output_type: The type of output.
        :param region: The region for the boto3 client.
        :param inputs: The input literal map to be used for updating the configuration.
        """
        super(SagemakerInvokeEndpointTask, self).__init__(
            name=name,
            task_config=BotoConfig(
                service="sagemaker-runtime",
                method="invoke_endpoint",
                config=config,
                region=region,
            ),
            inputs=inputs,
            output_type=dict[str, Union[str, output_type]],
            container_image=DefaultImages.default_image(),
            **kwargs,
        )
