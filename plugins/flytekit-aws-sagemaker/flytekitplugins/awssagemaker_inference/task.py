from typing import Any, Dict, Optional, Type, Union

from flytekit import ImageSpec, kwtypes
from flytekit.configuration import SerializationSettings
from flytekit.core.base_task import PythonTask
from flytekit.core.interface import Interface
from flytekit.extend.backend.base_agent import AsyncAgentExecutorMixin

from .boto3_task import BotoConfig, BotoTask


class SageMakerModelTask(BotoTask):
    def __init__(
        self,
        name: str,
        config: Dict[str, Any],
        region: Optional[str] = None,
        images: Optional[Dict[str, Union[str, ImageSpec]]] = None,
        inputs: Optional[Dict[str, Type]] = None,
        **kwargs,
    ):
        """
        Creates a SageMaker model.

        :param name: The name of the task.
        :param config: The configuration to be provided to the boto3 API call.
        :param region: The region for the boto3 client.
        :param images: Images for SageMaker model creation.
        :param inputs: The input literal map to be used for updating the configuration.
        """

        super(SageMakerModelTask, self).__init__(
            name=name,
            task_config=BotoConfig(
                service="sagemaker",
                method="create_model",
                config=config,
                region=region,
                images=images,
            ),
            inputs=inputs,
            **kwargs,
        )


class SageMakerEndpointConfigTask(BotoTask):
    def __init__(
        self,
        name: str,
        config: Dict[str, Any],
        region: Optional[str] = None,
        inputs: Optional[Dict[str, Type]] = None,
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
            **kwargs,
        )


class SageMakerEndpointTask(AsyncAgentExecutorMixin, PythonTask):
    _TASK_TYPE = "sagemaker-endpoint"

    def __init__(
        self,
        name: str,
        config: Dict[str, Any],
        region: Optional[str] = None,
        inputs: Optional[Dict[str, Type]] = None,
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
            task_type=self._TASK_TYPE,
            interface=Interface(inputs=inputs, outputs=kwtypes(result=dict)),
            **kwargs,
        )
        self._config = config
        self._region = region

    def get_custom(self, settings: SerializationSettings) -> Dict[str, Any]:
        return {"config": self._config, "region": self._region}


class SageMakerDeleteEndpointTask(BotoTask):
    def __init__(
        self,
        name: str,
        config: Dict[str, Any],
        region: Optional[str] = None,
        inputs: Optional[Dict[str, Type]] = None,
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
            **kwargs,
        )


class SageMakerDeleteEndpointConfigTask(BotoTask):
    def __init__(
        self,
        name: str,
        config: Dict[str, Any],
        region: Optional[str] = None,
        inputs: Optional[Dict[str, Type]] = None,
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
            **kwargs,
        )


class SageMakerDeleteModelTask(BotoTask):
    def __init__(
        self,
        name: str,
        config: Dict[str, Any],
        region: Optional[str] = None,
        inputs: Optional[Dict[str, Type]] = None,
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
            **kwargs,
        )


class SageMakerInvokeEndpointTask(BotoTask):
    def __init__(
        self,
        name: str,
        config: Dict[str, Any],
        region: Optional[str] = None,
        inputs: Optional[Dict[str, Type]] = None,
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
            **kwargs,
        )
