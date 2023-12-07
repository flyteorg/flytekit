import typing
from dataclasses import dataclass

import boto3
import botocore.exceptions


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

    def __init__(self, *, service: str, region: typing.Optional[str] = None, **kwargs):
        """
        :param service: The AWS service to use - e.g. sagemaker
        :param region: The region to use for the boto3 client, can be overridden when calling the boto3 method.
        """
        self._region = region
        self._service = service

    def _call(self, method: str, config: typing.Dict[str, typing.Any],
              args: typing.Optional[typing.Dict[str, typing.Any]] = None,
              region: typing.Optional[str] = None) -> typing.Dict[str, typing.Any]:
        """
        TODO we should probably also accept task_template and inputs separately, and then call update_dict
        Use this method to call any boto3 method (aws service method).
        :param method: The boto3 method to call - e.g. create_endpoint_config
        :param config: The config for the method - e.g. {"EndpointConfigName": "my-endpoint-config"}. The config can
        contain placeholders that will be replaced by values from the args dict. For example, if the config is
        {"EndpointConfigName": "{endpoint_config_name}"}, and the args dict is {"endpoint_config_name": "my-endpoint-config"},
        then the config will be updated to {"EndpointConfigName": "my-endpoint-config"} before calling the boto3 method.
        :param args: The args dict can be used to provide values for placeholders in the config dict. For example, if the config is
        :param region: The region to use for the boto3 client. If not provided, the region provided in the constructor will be used.
        """
        client = boto3.client(self._service, region)
        updated_config = update_dict(config, args or {})
        try:
            res = getattr(client, method)(**updated_config)
        except botocore.exceptions.ClientError as e:
            raise e
        return res
