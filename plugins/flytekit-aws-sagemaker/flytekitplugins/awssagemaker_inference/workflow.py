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


def create_deployment_task(
    name: str,
    task_type: Any,
    config: Dict[str, Any],
    region: str,
    inputs: Optional[Dict[str, Type]],
    images: Optional[Dict[str, Any]],
) -> Any:
    return task_type(name=name, config=config, region=region, inputs=inputs, images=images)


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

    wf = Workflow(name=f"sagemaker-deploy-{name}")

    if region_at_runtime:
        model_input_types.update({"region": str})
        endpoint_config_input_types.update({"region": str})
        endpoint_input_types.update({"region": str})
        wf.add_workflow_input("region", str)

    inputs = {
        SageMakerModelTask: {
            "input_types": model_input_types,
            "name": "sagemaker-model",
            "images": True,
            "config": model_config,
        },
        SageMakerEndpointConfigTask: {
            "input_types": endpoint_config_input_types,
            "name": "sagemaker-endpoint-config",
            "images": False,
            "config": endpoint_config_config,
        },
        SageMakerEndpointTask: {
            "input_types": endpoint_input_types,
            "name": "sagemaker-endpoint",
            "images": False,
            "config": endpoint_config,
        },
    }

    nodes = []
    for key, value in inputs.items():
        obj = create_deployment_task(
            name=f"{value['name']}-{name}",
            task_type=key,
            config=value["config"],
            region=region,
            inputs=value["input_types"],
            images=images if value["images"] else None,
        )
        input_dict = {}
        if isinstance(value, dict):
            for param, t in value.items():
                # Handles the scenario when the same input is present during different API calls.
                if param not in wf.inputs.keys():
                    wf.add_workflow_input(param, t)
                input_dict[param] = wf.inputs[param]
        node = wf.add_entity(obj, **input_dict)
        if len(nodes) > 0:
            nodes[-1] >> node
        nodes.append(node)

    wf.add_workflow_output("wf_output", nodes[2].outputs["result"], str)
    return wf


def create_delete_task(
    name: str,
    task_type: Any,
    config: Dict[str, Any],
    region: str,
    value: str,
    region_at_runtime: bool,
) -> Any:
    return task_type(
        name=name,
        config=config,
        region=region,
        inputs=(kwtypes(**{value: str, "region": str}) if region_at_runtime else kwtypes(value=str)),
    )


def delete_sagemaker_deployment(name: str, region: Optional[str] = None, region_at_runtime: bool = False):
    """
    Deletes SageMaker model, endpoint config and endpoint.

    :param name: The prefix to be added to the task names.
    :param region: The region to use for SageMaker API calls.
    :param region_at_runtime: Set this to True if you want to provide the region at runtime.
    """
    if not any((region, region_at_runtime)):
        raise ValueError("Region parameter is required.")

    wf = Workflow(name=f"sagemaker-delete-endpoint-wf-{name}")

    if region_at_runtime:
        wf.add_workflow_input("region", str)

    inputs = {
        SageMakerDeleteEndpointTask: "endpoint_name",
        SageMakerDeleteEndpointConfigTask: "endpoint_config_name",
        SageMakerDeleteModelTask: "model_name",
    }

    nodes = []
    for key, value in inputs.items():
        obj = create_delete_task(
            name=f"sagemaker-delete-{value.replace('_name').replace('_', '-')}-{name}",
            task_type=key,
            config={value.title().replace("_", ""): f"{{inputs.{value}}}"},
            region=region,
            value=value,
            region_at_runtime=region_at_runtime,
        )

        wf.add_workflow_input(value, str)
        node = wf.add_entity(
            obj,
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
