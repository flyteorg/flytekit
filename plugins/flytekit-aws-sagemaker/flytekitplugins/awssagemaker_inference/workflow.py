import re
from collections import OrderedDict
from typing import Any, Dict, Optional, Tuple, Type

from flytekit import Workflow, kwtypes, task

from .task import (
    SageMakerDeleteEndpointConfigTask,
    SageMakerDeleteEndpointTask,
    SageMakerDeleteModelTask,
    SageMakerEndpointConfigTask,
    SageMakerEndpointTask,
    SageMakerListEndpointConfigsTask,
    SageMakerListEndpointsTask,
    SageMakerListModelsTask,
    SageMakerModelTask,
)


@task
def is_params_exist(
    models: dict,
    endpoint_configs: dict,
    endpoints: dict,
    model_name: str,
    endpoint_config_name: str,
    endpoint_name: str,
) -> bool:
    """Model, endpoint config, and config must exist if the deployment needs to be deleted."""
    model_exists = any(model["ModelName"] == model_name for model in models["Models"])
    endpoint_config_exists = any(
        endpoint_config["EndpointConfigName"] == endpoint_config_name
        for endpoint_config in endpoint_configs["EndpointConfigs"]
    )
    endpoint_exists = any(endpoint["EndpointName"] == endpoint_name for endpoint in endpoints["Endpoints"])
    return model_exists and endpoint_config_exists and endpoint_exists


def is_deployment_exists(
    name: str,
    region: Optional[str] = None,
    region_at_runtime: bool = False,
):
    if not any((region, region_at_runtime)):
        raise ValueError("Region parameter is required.")

    wf = Workflow(name=f"sagemaker-check-if-deployment-exists-{name}")

    inputs = {
        "model_name": str,
        "endpoint_config": str,
        "endpoint": str,
    }

    if region_at_runtime:
        inputs["region"] = str

    for input, t in inputs.items():
        wf.add_workflow_input(input, t)

    tasks = {}
    for task_name, task_type, task_inputs in [
        ("sagemaker_list_models", SageMakerListModelsTask, {"model_name": str}),
        (
            "sagemaker_list_endpoint_configs",
            SageMakerListEndpointConfigsTask,
            {"endpoint_config_name": str},
        ),
        (
            "sagemaker_list_endpoints",
            SageMakerListEndpointsTask,
            {"endpoint_name": str},
        ),
    ]:
        config = {"NameContains": f"{{inputs.{task_inputs.keys()[0]}}}"}

        if region_at_runtime:
            task_inputs["region"] = str

        tasks[task_name] = task_type(
            name=f"{task_name}-{name}",
            config=config,
            region=region,
            inputs=kwtypes(**task_inputs),
        )

        input_vals = {}
        for input in inputs.keys():
            input_vals[input] = wf.inputs[input]

        wf.add_entity(tasks[task_name], **{key: wf.inputs[key] for key in task_inputs.keys()})

    node = wf.add_entity(
        is_params_exist,
        models=tasks["sagemaker_list_models"].outputs["o0"],
        endpoint_configs=tasks["sagemaker_list_endpoint_configs"].outputs["o0"],
        endpoints=tasks["sagemaker_list_endpoints"].outputs["o0"],
        **{key: wf.inputs[key] for key in inputs.keys()},
    )

    wf.add_workflow_output("wf_output", node.outputs["o0"], bool)

    return wf


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
        task_type(name=name, config=config, region=region, inputs=inputs, images=images),
        inputs,
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
    is_override: bool = False,
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
    :param is_override: Set this to True if you want to delete the existing deployment.
    """
    if not any((region, region_at_runtime)):
        raise ValueError("Region parameter is required.")

    wf = Workflow(name=f"sagemaker-deploy-{name}")

    if region_at_runtime:
        wf.add_workflow_input("region", str)

    if is_override:
        wf.add_workflow_input("override", bool)

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

    task_input_dict = []
    nodes = []
    for key, value in inputs.items():
        input_types = value["input_types"]
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
                # Handles the scenario when the same input is present during different API calls.
                if param not in wf.inputs.keys():
                    wf.add_workflow_input(param, t)
                input_dict[param] = wf.inputs[param]
        task_input_dict.append((obj, input_dict))

    if "override" in wf.inputs["override"] and wf.inputs["override"] is True:
        delete_sagemaker_deployment_wf = delete_sagemaker_deployment(
            name=name, region=region, region_at_runtime=region_at_runtime
        )

        inputs_mapping = {
            "endpoint_name": endpoint_config.get("EndpointName"),
            "endpoint_config_name": endpoint_config.get("EndpointConfigName"),
            "model_name": model_config.get("ModelName"),
        }

        pattern = r"\{(.*?)\.(\w+)\}"
        for key, value in inputs_mapping.items():
            if isinstance(value, str) and "inputs" in value:
                matches = re.search(pattern, value)
                if matches:
                    variable_name = matches.group(2)
                    inputs_mapping[key] = wf.inputs[variable_name]

        if region_at_runtime:
            inputs_mapping["region"] = wf.inputs["region"]

        wf.add_entity(delete_sagemaker_deployment_wf, **inputs_mapping)

    # This can be a part of the task_input_dict for loop, but >> operator doesn't work locally
    # https://github.com/flyteorg/flytekit/pull/1917
    for task_input_tuple in task_input_dict:
        node = wf.add_entity(task_input_tuple[0], **task_input_tuple[1])
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

    inputs = OrderedDict(
        [
            (SageMakerDeleteEndpointTask, "endpoint_name"),
            (SageMakerDeleteEndpointConfigTask, "endpoint_config_name"),
            (SageMakerDeleteModelTask, "model_name"),
        ]
    )

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
