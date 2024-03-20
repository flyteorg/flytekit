from typing import Any, Dict, Optional, Type

from flytekit import Workflow, kwtypes

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
    model_config: Dict[str, Any],
    endpoint_config_config: Dict[str, Any],
    endpoint_config: Dict[str, Any],
    images: Optional[Dict[str, Any]] = None,
    model_input_types: Optional[Dict[str, Type]] = None,
    endpoint_config_input_types: Optional[Dict[str, Type]] = None,
    endpoint_input_types: Optional[Dict[str, Type]] = None,
    region: Optional[str] = None,
    region_at_runtime: bool = False,
):
    """
    Creates SageMaker model, endpoint config and endpoint.

    :param model_config: Configuration for the SageMaker model creation API call.
    :param endpoint_config_config: Configuration for the SageMaker endpoint configuration creation API call.
    :param endpoint_config: Configuration for the SageMaker endpoint creation API call.
    :param images: A dictionary of images for SageMaker model creation.
    :param model_input_types: Mapping of SageMaker model configuration inputs to their types.
    :param endpoint_config_input_types: Mapping of SageMaker endpoint configuration inputs to their types.
    :param endpoint_input_types: Mapping of SageMaker endpoint inputs to their types.
    :param region: The region for SageMaker API calls.
    :param region_at_runtime: Set this to True if you want to provide the region at runtime.
    """
    if not any((region, region_at_runtime)):
        raise ValueError("Region parameter is required.")

    sagemaker_model_task = SageMakerModelTask(
        name=f"sagemaker-model-{name}",
        config=model_config,
        region=region,
        inputs=(model_input_types.update({"region": str}) if region_at_runtime else model_input_types),
        images=images,
    )

    endpoint_config_task = SageMakerEndpointConfigTask(
        name=f"sagemaker-endpoint-config-{name}",
        config=endpoint_config_config,
        region=region,
        inputs=(
            endpoint_config_input_types.update({"region": str}) if region_at_runtime else endpoint_config_input_types
        ),
    )

    endpoint_task = SageMakerEndpointTask(
        name=f"sagemaker-endpoint-{name}",
        config=endpoint_config,
        region=region,
        inputs=(endpoint_input_types.update({"region": str}) if region_at_runtime else endpoint_input_types),
    )

    wf = Workflow(name=f"sagemaker-deploy-{name}")

    inputs = {
        sagemaker_model_task: model_input_types,
        endpoint_config_task: endpoint_config_input_types,
        endpoint_task: endpoint_input_types,
    }

    if region_at_runtime:
        wf.add_workflow_input("region", str)

    nodes = []
    for key, value in inputs.items():
        input_dict = {}
        if isinstance(value, dict):
            for param, t in value.items():
                # Handles the scenario when the same input is present during different API calls.
                if param not in wf.inputs.keys():
                    wf.add_workflow_input(param, t)
                input_dict[param] = wf.inputs[param]
        if region_at_runtime:
            input_dict["region"] = wf.inputs["region"]
        node = wf.add_entity(key, **input_dict)
        if len(nodes) > 0:
            nodes[-1] >> node
        nodes.append(node)

    wf.add_workflow_output("wf_output", nodes[2].outputs["result"], str)
    return wf


def delete_sagemaker_deployment(name: str, region: Optional[str] = None, region_at_runtime: bool = False):
    """
    Deletes SageMaker model, endpoint config and endpoint.

    :param name: The prefix to be added to the task names.
    :param region: The region to use for SageMaker API calls.
    :param region_at_runtime: Set this to True if you want to provide the region at runtime.
    """
    if not any((region, region_at_runtime)):
        raise ValueError("Region parameter is required.")

    sagemaker_delete_endpoint = SageMakerDeleteEndpointTask(
        name=f"sagemaker-delete-endpoint-{name}",
        config={"EndpointName": "{inputs.endpoint_name}"},
        region=region,
        inputs=(kwtypes(endpoint_name=str, region=str) if region_at_runtime else kwtypes(endpoint_name=str)),
    )

    sagemaker_delete_endpoint_config = SageMakerDeleteEndpointConfigTask(
        name=f"sagemaker-delete-endpoint-config-{name}",
        config={"EndpointConfigName": "{inputs.endpoint_config_name}"},
        region=region,
        inputs=(
            kwtypes(endpoint_config_name=str, region=str) if region_at_runtime else kwtypes(endpoint_config_name=str)
        ),
    )

    sagemaker_delete_model = SageMakerDeleteModelTask(
        name=f"sagemaker-delete-model-{name}",
        config={"ModelName": "{inputs.model_name}"},
        region=region,
        inputs=(kwtypes(model_name=str, region=str) if region_at_runtime else kwtypes(model_name=str)),
    )

    wf = Workflow(name=f"sagemaker-delete-endpoint-wf-{name}")

    if region_at_runtime:
        wf.add_workflow_input("region", str)

    inputs = {
        sagemaker_delete_endpoint: "endpoint_name",
        sagemaker_delete_endpoint_config: "endpoint_config_name",
        sagemaker_delete_model: "model_name",
    }

    nodes = []
    for key, value in inputs.items():
        wf.add_workflow_input(value, str)
        node = wf.add_entity(
            key,
            **(
                {
                    value: wf.inputs[value],
                    "region": wf.inputs["region"],
                }
                if region_at_runtime
                else {value: wf.inputs[value]}
            ),
        )
        if len(nodes) > 0:
            nodes[-1] >> node
        nodes.append(node)

    return wf
