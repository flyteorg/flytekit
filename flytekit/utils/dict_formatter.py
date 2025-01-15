import re
from typing import Any, Dict, Optional


def get_nested_value(d: Dict[str, Any], keys: list[str]) -> Any:
    """
    Retrieve the nested value from a dictionary based on a list of keys.
    """
    for key in keys:
        if key not in d:
            raise ValueError(f"Could not find the key {key} in {d}.")
        d = d[key]
    return d


def replace_placeholder(
    service: str,
    original_dict: str,
    placeholder: str,
    replacement: str,
) -> str:
    """
    Replace a placeholder in the original string and handle the specific logic for the sagemaker service and idempotence token.
    """
    temp_dict = original_dict.replace(f"{{{placeholder}}}", replacement)
    if service == "sagemaker" and placeholder in [
        "inputs.idempotence_token",
        "idempotence_token",
    ]:
        if len(temp_dict) > 63:
            truncated_token = replacement[: 63 - len(original_dict.replace(f"{{{placeholder}}}", ""))]
            return original_dict.replace(f"{{{placeholder}}}", truncated_token)
        else:
            return temp_dict
    return temp_dict


def format_dict(
    service: str,
    original_dict: Any,
    update_dict: Dict[str, Any],
    idempotence_token: Optional[str] = None,
) -> Any:
    """
    Recursively update a dictionary with format strings with values from another dictionary where the keys match
    the format string. This goes a little beyond regular python string formatting and uses `.` to denote nested keys.

    For example, if original_dict is {"EndpointConfigName": "{endpoint_config_name}"},
    and update_dict is {"endpoint_config_name": "my-endpoint-config"},
    then the result will be {"EndpointConfigName": "my-endpoint-config"}.

    For nested keys if the original_dict is {"EndpointConfigName": "{inputs.endpoint_config_name}"},
    and update_dict is {"inputs": {"endpoint_config_name": "my-endpoint-config"}},
    then the result will be {"EndpointConfigName": "my-endpoint-config"}.

    :param service: The AWS service to use
    :param original_dict: The dictionary to update (in place)
    :param update_dict: The dictionary to use for updating
    :param idempotence_token: Hash of config -- this is to ensure the execution ID is deterministic
    :return: The updated dictionary
    """
    if original_dict is None:
        return None

    if isinstance(original_dict, str) and "{" in original_dict and "}" in original_dict:
        matches = re.findall(r"\{([^}]+)\}", original_dict)
        for match in matches:
            if "." in match:
                keys = match.split(".")
                nested_value = get_nested_value(update_dict, keys)
                if f"{{{match}}}" == original_dict:
                    return nested_value
                else:
                    original_dict = replace_placeholder(service, original_dict, match, str(nested_value))
            elif match == "idempotence_token" and idempotence_token:
                original_dict = replace_placeholder(service, original_dict, match, idempotence_token)
        return original_dict

    if isinstance(original_dict, list):
        return [format_dict(service, item, update_dict, idempotence_token) for item in original_dict]

    if isinstance(original_dict, dict):
        for key, value in original_dict.items():
            original_dict[key] = format_dict(service, value, update_dict, idempotence_token)

    return original_dict
