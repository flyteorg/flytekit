from flytekit import Workflow, kwtypes, ImageSpec
from .task import (
    SagemakerModelTask,
    SagemakerEndpointConfigTask,
    SagemakerDeleteEndpointTask,
    SagemakerDeleteEndpointConfigTask,
    SagemakerDeleteModelTask,
    SagemakerEndpointTask,
)

from typing import Any, Optional, Union, Type


def create_sagemaker_deployment(
    model_name: str,
    model_config: dict[str, Any],
    endpoint_config_config: dict[str, Any],
    endpoint_config: dict[str, Any],
    model_input_types: Optional[dict[str, Type]] = None,
    endpoint_config_input_types: Optional[dict[str, Type]] = None,
    endpoint_input_types: Optional[dict[str, Type]] = None,
    container_image: Optional[Union[str, ImageSpec]] = None,
    region: Optional[str] = None,
):
    """
    Creates Sagemaker model, endpoint config and endpoint.
    """
    sagemaker_model_task = SagemakerModelTask(
        name=f"sagemaker-model-{model_name}",
        config=model_config,
        region=region,
        inputs=model_input_types,
        container_image=container_image,
    )

    endpoint_config_task = SagemakerEndpointConfigTask(
        name=f"sagemaker-endpoint-config-{model_name}",
        config=endpoint_config_config,
        region=region,
        inputs=endpoint_config_input_types,
    )

    endpoint_task = SagemakerEndpointTask(
        name=f"sagemaker-endpoint-{model_name}",
        config=endpoint_config,
        region=region,
        inputs=endpoint_input_types,
    )

    wf = Workflow(name=f"sagemaker-deploy-{model_name}")
    wf.add_workflow_input("model_inputs", Optional[dict])
    wf.add_workflow_input("endpoint_config_inputs", Optional[dict])
    wf.add_workflow_input("endpoint_inputs", Optional[dict])

    wf.add_entity(
        sagemaker_model_task,
        **wf.inputs["model_inputs"],
    )

    wf.add_entity(
        endpoint_config_task,
        **wf.inputs["endpoint_config_inputs"],
    )

    wf.add_entity(endpoint_task, **wf.inputs["endpoint_inputs"])

    return wf


def delete_sagemaker_deployment(name: str, region: Optional[str] = None):
    """
    Deletes Sagemaker model, endpoint config and endpoint.
    """
    sagemaker_delete_endpoint = SagemakerDeleteEndpointTask(
        name=f"sagemaker-delete-endpoint-{name}",
        config={"EndpointName": "{endpoint_name}"},
        region=region,
        inputs=kwtypes(endpoint_name=str),
    )

    sagemaker_delete_endpoint_config = SagemakerDeleteEndpointConfigTask(
        name=f"sagemaker-delete-endpoint-config-{name}",
        config={"EndpointConfigName": "{endpoint_config_name}"},
        region=region,
        inputs=kwtypes(endpoint_config_name=str),
    )

    sagemaker_delete_model = SagemakerDeleteModelTask(
        name=f"sagemaker-delete-model-{name}",
        config={"ModelName": "{model_name}"},
        region=region,
        inputs=kwtypes(model_name=str),
    )

    wf = Workflow(name=f"sagemaker-delete-endpoint-wf-{name}")
    wf.add_workflow_input("endpoint_name", str)
    wf.add_workflow_input("endpoint_config_name", str)
    wf.add_workflow_input("model_name", str)

    wf.add_entity(
        sagemaker_delete_endpoint,
        **{"endpoint_name": wf.inputs["endpoint_name"]},
    )
    wf.add_entity(
        sagemaker_delete_endpoint_config,
        **{"endpoint_config_name": wf.inputs["endpoint_config_name"]},
    )
    wf.add_entity(
        sagemaker_delete_model,
        **{"model_name", wf.inputs["model_name"]},
    )

    return wf
