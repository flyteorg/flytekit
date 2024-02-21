from typing import Any, Optional, Type, Union

from flytekit import ImageSpec, Workflow, kwtypes

from .task import (
    SageMakerDeleteEndpointConfigTask,
    SageMakerDeleteEndpointTask,
    SageMakerDeleteModelTask,
    SageMakerEndpointConfigTask,
    SageMakerEndpointTask,
    SageMakerModelTask,
)


def create_sagemaker_deployment(
    name: str,
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
    Creates SageMaker model, endpoint config and endpoint.
    """
    sagemaker_model_task = SageMakerModelTask(
        name=f"sagemaker-model-{name}",
        config=model_config,
        region=region,
        inputs=model_input_types,
        container_image=container_image,
    )

    endpoint_config_task = SageMakerEndpointConfigTask(
        name=f"sagemaker-endpoint-config-{name}",
        config=endpoint_config_config,
        region=region,
        inputs=endpoint_config_input_types,
    )

    endpoint_task = SageMakerEndpointTask(
        name=f"sagemaker-endpoint-{name}",
        config=endpoint_config,
        region=region,
        inputs=endpoint_input_types,
    )

    wf = Workflow(name=f"sagemaker-deploy-{name}")

    inputs = {
        sagemaker_model_task: model_input_types,
        endpoint_config_task: endpoint_config_input_types,
        endpoint_task: endpoint_input_types,
    }

    nodes = []
    for key, value in inputs.items():
        input_dict = {}
        if isinstance(value, dict):
            for param, t in value.items():
                wf.add_workflow_input(param, t)
                input_dict[param] = wf.inputs[param]
        nodes.append(wf.add_entity(key, **input_dict))

    wf.add_workflow_output("wf_output", nodes[2].outputs["result"], str)
    return wf


def delete_sagemaker_deployment(name: str, region: Optional[str] = None):
    """
    Deletes SageMaker model, endpoint config and endpoint.
    """
    sagemaker_delete_endpoint = SageMakerDeleteEndpointTask(
        name=f"sagemaker-delete-endpoint-{name}",
        config={"EndpointName": "{inputs.endpoint_name}"},
        region=region,
        inputs=kwtypes(endpoint_name=str),
    )

    sagemaker_delete_endpoint_config = SageMakerDeleteEndpointConfigTask(
        name=f"sagemaker-delete-endpoint-config-{name}",
        config={"EndpointConfigName": "{inputs.endpoint_config_name}"},
        region=region,
        inputs=kwtypes(endpoint_config_name=str),
    )

    sagemaker_delete_model = SageMakerDeleteModelTask(
        name=f"sagemaker-delete-model-{name}",
        config={"ModelName": "{inputs.model_name}"},
        region=region,
        inputs=kwtypes(model_name=str),
    )

    wf = Workflow(name=f"sagemaker-delete-endpoint-wf-{name}")
    wf.add_workflow_input("endpoint_name", str)
    wf.add_workflow_input("endpoint_config_name", str)
    wf.add_workflow_input("model_name", str)

    wf.add_entity(
        sagemaker_delete_endpoint,
        endpoint_name=wf.inputs["endpoint_name"],
    )
    wf.add_entity(
        sagemaker_delete_endpoint_config,
        endpoint_config_name=wf.inputs["endpoint_config_name"],
    )
    wf.add_entity(
        sagemaker_delete_model,
        model_name=wf.inputs["model_name"],
    )

    return wf
