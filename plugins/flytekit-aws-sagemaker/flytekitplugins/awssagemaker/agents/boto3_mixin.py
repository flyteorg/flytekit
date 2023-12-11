import typing

import boto3
import botocore.exceptions
from flyteidl.core.tasks_pb2 import TaskTemplate
from google.protobuf.json_format import MessageToDict

from flytekit.interaction.string_literals import literal_map_string_repr
from flytekit.models.literals import LiteralMap


def update_dict(d: typing.Any, u: typing.Dict[str, typing.Any]) -> typing.Any:
    """
    Recursively update a dictionary with values from another dictionary. E.g. if d is {"EndpointConfigName": "{endpoint_config_name}"},
    and u is {"endpoint_config_name": "my-endpoint-config"}, then the result will be {"EndpointConfigName": "my-endpoint-config"}.
    :param d: The dictionary to update (in place)
    :param u: The dictionary to use for updating
    :return: The updated dictionary - Note that the original dictionary is updated in place.
    """
    if d is None:
        return None
    if isinstance(d, str):
        if "{" in d and "}" in d:
            v = d.format(**u)
            if v == d:
                raise ValueError(f"Could not find value for {d}")
            orig_v = u.get(d.replace("{", "").replace("}", ""))
            if isinstance(orig_v, str):
                return v
            return orig_v
        return d
    if isinstance(d, list):
        return [update_dict(i, u) for i in d]
    if isinstance(d, dict):
        for k, v in d.items():
            d[k] = update_dict(d.get(k), u)
    return d


# TODO write AsyncBoto3AgentBase - https://github.com/terrycain/aioboto3
class Boto3AgentMixin:
    """
    This mixin can be used to create a Flyte agent for any AWS service, using boto3.
    The mixin provides a single method `_call` that can be used to call any boto3 method.
    """

    def __init__(self, *, service: typing.Optional[str] = None, region: typing.Optional[str] = None, **kwargs):
        """
        :param service: The AWS service to use - e.g. sagemaker
        :param region: The region to use for the boto3 client, can be overridden when calling the boto3 method.
        """
        self._region = region
        self._service = service

    def _call(self, method: str, config: typing.Dict[str, typing.Any],
              task_template: TaskTemplate,
              inputs: typing.Optional[LiteralMap] = None,
              additional_args: typing.Optional[typing.Dict[str, typing.Any]] = None,
              region: typing.Optional[str] = None) -> typing.Dict[str, typing.Any]:
        """
        TODO we should probably also accept task_template and inputs separately, and then call update_dict
        Use this method to call any boto3 method (aws service method).
        :param method: The boto3 method to call - e.g. create_endpoint_config
        :param config: The config for the method - e.g. {"EndpointConfigName": "my-endpoint-config"}. The config can
        contain placeholders that will be replaced by values from the inputs, task_template or additional_args.
        For example, if the config is
        {"EndpointConfigName": "{inputs.endpoint_config_name}", "EndpointName": "{endpoint_name}",
         "Image": "{container.image}"}
        and the additional_args dict is {"endpoint_name": "my-endpoint"}, the inputs contains a string literal for
        endpoint_config_name and the task_template contains a container with an image, then the config will be updated
         to {"EndpointConfigName": "my-endpoint-config", "EndpointName": "my-endpoint", "Image": "my-image"}
         before calling the boto3 method.
        :param task_template: The task template for the task that is being created.
        :param inputs: The inputs for the task that is being created.
        :param additional_args: Additional arguments to use for updating the config. These are optional and
        can be controlled by the task author.
        :param region: The region to use for the boto3 client. If not provided, the region provided in the constructor will be used.
        """
        client = boto3.client(self._service, region)
        args = {}
        if inputs:
            args["inputs"] = literal_map_string_repr(inputs)
        if task_template:
            args["container"] = MessageToDict(task_template.container)
        if additional_args:
            args.update(additional_args)
        updated_config = update_dict(config, args)
        try:
            res = getattr(client, method)(**updated_config)
        except botocore.exceptions.ClientError as e:
            raise e
        return res
