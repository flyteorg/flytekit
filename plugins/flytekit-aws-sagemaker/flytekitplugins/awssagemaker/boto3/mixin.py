from typing import Any, Optional

import aioboto3
from flyteidl.core.tasks_pb2 import TaskTemplate
from google.protobuf.json_format import MessageToDict

from flytekit.interaction.string_literals import literal_map_string_repr
from flytekit.models.literals import LiteralMap


class AttrDict(dict):
    """
    This class converts a dictionary into an attribute-style lookup. It is specifically designed for
    namespacing inputs and outputs, providing a convenient way to access dictionary elements using dot notation.
    """

    def __init__(self, *args, **kwargs):
        super(AttrDict, self).__init__(*args, **kwargs)
        self.__dict__ = self


def update_dict_fn(original_dict: Any, update_dict: dict[str, Any]) -> Any:
    """
    Recursively update a dictionary with values from another dictionary.
    For example, if original_dict is {"EndpointConfigName": "{endpoint_config_name}"},
    and update_dict is {"endpoint_config_name": "my-endpoint-config"},
    then the result will be {"EndpointConfigName": "my-endpoint-config"}.

    :param original_dict: The dictionary to update (in place)
    :param update_dict: The dictionary to use for updating
    :return: The updated dictionary - note that the original dictionary is updated in place
    """
    if original_dict is None:
        return None

    # If the original value is a string
    if isinstance(original_dict, str):
        # If the string contains placeholder curly braces, replace the placeholder with the actual value
        if "{" in original_dict and "}" in original_dict:
            try:
                return original_dict.format(**update_dict)
            except KeyError as e:
                raise ValueError(f"Variable {e} in placeholder not found in inputs {update_dict.keys()}")

        # If the string does not contain placeholders, return it as is
        return original_dict

    # If the original value is a list, recursively update each element in the list
    if isinstance(original_dict, list):
        return [update_dict_fn(item, update_dict) for item in original_dict]

    # If the original value is a dictionary, recursively update each key-value pair
    if isinstance(original_dict, dict):
        for key, value in original_dict.items():
            original_dict[key] = update_dict_fn(value, update_dict)

    # Return the updated original dict
    return original_dict


class Boto3AgentMixin:
    """
    This mixin facilitates the creation of a Flyte agent for any AWS service using boto3.
    It provides a single method, `_call`, which can be employed to invoke any boto3 method.
    """

    def __init__(self, *, service: str, method: str, region: Optional[str] = None, **kwargs):
        """
        Initialize the Boto3AgentMixin.

        :param service: The AWS service to use, e.g., sagemaker.
        :param region: The region for the boto3 client; can be overridden when calling boto3 methods.
        """
        self._service = service
        self._region = region
        self._method = method
        super().__init__(**kwargs)

    async def _call(
        self,
        config: dict[str, Any],
        task_template: Optional[TaskTemplate] = None,
        inputs: Optional[LiteralMap] = None,
        additional_args: Optional[dict[str, Any]] = None,
        region: Optional[str] = None,
        aws_access_key_id: Optional[str] = None,
        aws_secret_access_key: Optional[str] = None,
        aws_session_token: Optional[str] = None,
    ) -> Any:
        """
        Utilize this method to invoke any boto3 method (AWS service method).

        :param method: The boto3 method to invoke, e.g., create_endpoint_config.
        :param config: The configuration for the method, e.g., {"EndpointConfigName": "my-endpoint-config"}. The config
        may contain placeholders replaced by values from inputs, task_template, or additional_args.
        For example, if the config is
        {"EndpointConfigName": "{inputs.endpoint_config_name}", "EndpointName": "{endpoint_name}",
         "Image": "{container.image}"}
        and the additional_args dict is {"endpoint_name": "my-endpoint"}, the inputs contain a string literal for
        endpoint_config_name, and the task_template contains a container with an image,
        then the config will be updated to {"EndpointConfigName": "my-endpoint-config", "EndpointName": "my-endpoint",
         "Image": "my-image"} before invoking the boto3 method.
        :param task_template: The task template for the task being created.
        :param inputs: The inputs for the task being created.
        :param additional_args: Additional arguments for updating the config. These are optional and can be controlled by the task author.
        :param region: The region for the boto3 client. If not provided, the region specified in the constructor will be used.
        """
        args = {}
        if inputs:
            args["inputs"] = AttrDict(literal_map_string_repr(inputs))
        if task_template:
            args["container"] = AttrDict(MessageToDict(task_template.container))
        if additional_args:
            args.update(additional_args)

        updated_config = update_dict_fn(config, args)

        session = aioboto3.Session()
        async with session.client(
            service_name=self._service,
            region_name=region,
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key,
            aws_session_token=aws_session_token,
        ) as client:
            try:
                result = await getattr(client, self._method)(**updated_config)
            except Exception as e:
                raise e

        return result
