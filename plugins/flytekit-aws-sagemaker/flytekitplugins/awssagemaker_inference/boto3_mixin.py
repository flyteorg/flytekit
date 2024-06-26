from typing import Any, Dict, Optional

import aioboto3

from flytekit.interaction.string_literals import literal_map_string_repr
from flytekit.models.literals import LiteralMap

account_id_map = {
    "us-east-1": "785573368785",
    "us-east-2": "007439368137",
    "us-west-1": "710691900526",
    "us-west-2": "301217895009",
    "eu-west-1": "802834080501",
    "eu-west-2": "205493899709",
    "eu-west-3": "254080097072",
    "eu-north-1": "601324751636",
    "eu-south-1": "966458181534",
    "eu-central-1": "746233611703",
    "ap-east-1": "110948597952",
    "ap-south-1": "763008648453",
    "ap-northeast-1": "941853720454",
    "ap-northeast-2": "151534178276",
    "ap-southeast-1": "324986816169",
    "ap-southeast-2": "355873309152",
    "cn-northwest-1": "474822919863",
    "cn-north-1": "472730292857",
    "sa-east-1": "756306329178",
    "ca-central-1": "464438896020",
    "me-south-1": "836785723513",
    "af-south-1": "774647643957",
}


def update_dict_fn(original_dict: Any, update_dict: Dict[str, Any]) -> Any:
    """
    Recursively update a dictionary with values from another dictionary.
    For example, if original_dict is {"EndpointConfigName": "{endpoint_config_name}"},
    and update_dict is {"endpoint_config_name": "my-endpoint-config"},
    then the result will be {"EndpointConfigName": "my-endpoint-config"}.

    :param original_dict: The dictionary to update (in place)
    :param update_dict: The dictionary to use for updating
    :return: The updated dictionary
    """
    if original_dict is None:
        return None

    # If the original value is a string and contains placeholder curly braces
    if isinstance(original_dict, str):
        if "{" in original_dict and "}" in original_dict:
            # Check if there are nested keys
            if "." in original_dict:
                # Create a copy of update_dict
                update_dict_copy = update_dict.copy()

                # Fetch keys from the original_dict
                keys = original_dict.strip("{}").split(".")

                # Get value from the nested dictionary
                for key in keys:
                    try:
                        update_dict_copy = update_dict_copy[key]
                    except Exception:
                        raise ValueError(f"Could not find the key {key} in {update_dict_copy}.")

                return update_dict_copy

            # Retrieve the original value using the key without curly braces
            original_value = update_dict.get(original_dict.strip("{}"))

            # Check if original_value exists; if so, return it,
            # otherwise, raise a ValueError indicating that the value for the key original_dict could not be found.
            if original_value:
                return original_value
            else:
                raise ValueError(f"Could not find value for {original_dict}.")

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
    This mixin facilitates the creation of a Boto3 agent for any AWS service.
    It provides a single method, `_call`, which can be employed to invoke any Boto3 method.
    """

    def __init__(self, *, service: str, region: Optional[str] = None, **kwargs):
        """
        Initialize the Boto3AgentMixin.

        :param service: The AWS service to use, e.g., sagemaker.
        :param region: The region for the boto3 client; can be overridden when calling boto3 methods.
        """
        self._service = service
        self._region = region

        super().__init__(**kwargs)

    async def _call(
        self,
        method: str,
        config: Dict[str, Any],
        images: Optional[Dict[str, str]] = None,
        inputs: Optional[LiteralMap] = None,
        region: Optional[str] = None,
    ) -> Any:
        """
        Utilize this method to invoke any boto3 method (AWS service method).

        :param method: The boto3 method to invoke, e.g., create_endpoint_config.
        :param config: The configuration for the method, e.g., {"EndpointConfigName": "my-endpoint-config"}. The config
        may contain placeholders replaced by values from inputs.
        For example, if the config is
        {"EndpointConfigName": "{inputs.endpoint_config_name}", "EndpointName": "{inputs.endpoint_name}",
         "Image": "{images.primary_container_image}"},
        the inputs contain a string literal for endpoint_config_name and endpoint_name and images contain primary_container_image,
        then the config will be updated to {"EndpointConfigName": "my-endpoint-config", "EndpointName": "my-endpoint",
         "Image": "my-image"} before invoking the boto3 method.
        :param images: A dict of Docker images to use, for example, when deploying a model on SageMaker.
        :param inputs: The inputs for the task being created.
        :param region: The region for the boto3 client. If not provided, the region specified in the constructor will be used.
        """
        args = {}
        input_region = None

        if inputs:
            args["inputs"] = literal_map_string_repr(inputs)
            input_region = args["inputs"].get("region")

        final_region = input_region or region or self._region
        if not final_region:
            raise ValueError("Region parameter is required.")

        if images:
            base = "amazonaws.com.cn" if final_region.startswith("cn-") else "amazonaws.com"
            images = {
                image_name: (
                    image.format(
                        account_id=account_id_map[final_region],
                        region=final_region,
                        base=base,
                    )
                    if isinstance(image, str) and "sagemaker-tritonserver" in image
                    else image
                )
                for image_name, image in images.items()
            }
            args["images"] = images

        updated_config = update_dict_fn(config, args)

        # Asynchronous Boto3 session
        session = aioboto3.Session()
        async with session.client(
            service_name=self._service,
            region_name=final_region,
        ) as client:
            try:
                result = await getattr(client, method)(**updated_config)
            except Exception as e:
                raise e

        return result
