from typing import Any, Dict, Optional, Tuple, Type

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
    region_at_runtime: bool,
) -> Tuple[Any, Optional[Dict[str, Type]]]:
    if region_at_runtime:
        if inputs:
            inputs.update({"region": str})
        else:
            inputs = kwtypes(region=str)
    return (
        task_type(
            name=name,
            config=config,
            region=region,
            inputs=inputs,
            images=images,
        ),
        inputs,
    )


def append_token(config, key, token, name):
    if key in config:
        config[key] += f"-{{{token}}}"
    else:
        config[key] = f"{name}-{{{token}}}"


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
    idempotence_token: bool = True,
) -> Workflow:
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
    :param idempotence_token: Set this to False if you don't want the agent to automatically append a token/hash to the deployment names.
    """
    if not any((region, region_at_runtime)):
        raise ValueError("Region parameter is required.")

    wf = Workflow(name=f"sagemaker-deploy-{name}")

    if region_at_runtime:
        wf.add_workflow_input("region", str)

    if idempotence_token:
        append_token(model_config, "ModelName", "idempotence_token", name)
        append_token(endpoint_config_config, "EndpointConfigName", "idempotence_token", name)

        if "ProductionVariants" in endpoint_config_config and endpoint_config_config["ProductionVariants"]:
            append_token(
                endpoint_config_config["ProductionVariants"][0],
                "ModelName",
                "inputs.idempotence_token",
                name,
            )

        append_token(endpoint_config, "EndpointName", "idempotence_token", name)
        append_token(endpoint_config, "EndpointConfigName", "inputs.idempotence_token", name)

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
        input_types = value["input_types"]
        if len(nodes) > 0:
            if not input_types:
                input_types = {}
            input_types["idempotence_token"] = str

        obj, new_input_types = create_deployment_task(
            name=f"{value['name']}-{name}",
            task_type=key,
            config=value["config"],
            region=region,
            inputs=input_types,
            images=images if value["images"] else None,
            region_at_runtime=region_at_runtime,
        )
        input_dict = {}
        if isinstance(new_input_types, dict):
            for param, t in new_input_types.items():
                if param != "idempotence_token":
                    # Handles the scenario when the same input is present during different API calls.
                    if param not in wf.inputs.keys():
                        wf.add_workflow_input(param, t)
                    input_dict[param] = wf.inputs[param]
                else:
                    input_dict["idempotence_token"] = nodes[-1].outputs["idempotence_token"]

        node = wf.add_entity(obj, **input_dict)

        if len(nodes) > 0:
            nodes[-1] >> node
        nodes.append(node)

    wf.add_workflow_output(
        "wf_output",
        [
            nodes[0].outputs["result"],
            nodes[1].outputs["result"],
            nodes[2].outputs["result"],
        ],
        list[dict],
    )
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
        inputs=(kwtypes(**{value: str, "region": str}) if region_at_runtime else kwtypes(**{value: str})),
    )


def delete_sagemaker_deployment(name: str, region: Optional[str] = None, region_at_runtime: bool = False) -> Workflow:
    """
    Deletes SageMaker model, endpoint config and endpoint.

    :param name: The prefix to be added to the task names.
    :param region: The region to use for SageMaker API calls.
    :param region_at_runtime: Set this to True if you want to provide the region at runtime.
    """
    if not any((region, region_at_runtime)):
        raise ValueError("Region parameter is required.")

    wf = Workflow(name=f"sagemaker-delete-deployment-{name}")

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
            name=f"sagemaker-delete-{value.replace('_name', '').replace('_', '-')}-{name}",
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
