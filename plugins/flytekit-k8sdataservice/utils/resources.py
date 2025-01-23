from kubernetes import client


def cleanup_resources(resources: client.V1ResourceRequirements):
    filtered_resources_limits = {k: v for k, v in resources.limits.items() if not is_zero_like(v)}
    filtered_resources_requests = {k: v for k, v in resources.requests.items() if not is_zero_like(v)}
    resources.limits = filtered_resources_limits
    resources.requests = filtered_resources_requests


# We have noticed that for the fields that are not specified, there will be 0 or "0" filled in
# this is helper function to filter those values out.
def is_zero_like(value):
    return (
        value is None
        or value == "0"
        or value == 0
        or str(value).strip().lower() == "0"
        or str(value).strip().lower() == "0gb"
    )


def convert_flyte_to_k8s_fields(resources_dict):
    return {("memory" if k == "mem" else k): v for k, v in resources_dict.items()}
