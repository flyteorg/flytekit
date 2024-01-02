from flytekit import Workflow, kwtypes, LaunchPlan
from .boto3.agent import SyncBotoAgentTask

from flytekit.models import literals

from .task import SagemakerEndpointTask
from typing import Any, Optional, Union, Type


def create_sagemaker_deployment(
    model_name: str,
    model_config: dict[str, Any],
    endpoint_config_config: dict[str, Any],
    endpoint_config: dict[str, Any],
    region: Optional[str] = None,
    model_additional_args: Optional[dict[str, Any]] = None,
    endpoint_config_additional_args: Optional[dict[str, Any]] = None,
):
    sagemaker_model_task = SyncBotoAgentTask(
        name=f"sagemaker-model-{model_name}",
        config=model_config,
        service="sagemaker",
        method="create_model",
        region=region,
    )

    endpoint_config_task = SyncBotoAgentTask(
        name=f"sagemaker-endpoint-config-{model_name}",
        config=endpoint_config_config,
        service="sagemaker",
        method="create_endpoint_config",
        region=region,
    )

    endpoint_task = SagemakerEndpointTask(
        name=f"sagemaker-endpoint-{model_name}",
        task_config=endpoint_config,
        inputs=kwtypes(inputs=dict),
    )

    wf = Workflow(name=f"sagemaker-deploy-{model_name}")
    wf.add_workflow_input("model_inputs", dict)
    wf.add_workflow_input("endpoint_config_inputs", dict)
    wf.add_workflow_input("endpoint_inputs", dict)

    wf.add_entity(
        sagemaker_model_task,
        output_result_type=dict[str, str],
        inputs=wf.inputs["model_inputs"],
        additional_args=model_additional_args,
    )

    wf.add_entity(
        endpoint_config_task,
        output_result_type=dict[str, str],
        inputs=wf.inputs["endpoint_config_inputs"],
        additional_args=endpoint_config_additional_args,
    )

    wf.add_entity(endpoint_task, inputs=wf.inputs["endpoint_inputs"])

    lp = LaunchPlan.get_or_create(
        workflow=wf,
        default_inputs={
            "model_inputs": None,
            "endpoint_config_inputs": None,
            "endpoint_status": None,
        },
    )
    return lp


def delete_sagemaker_deployment(name: str, region: Optional[str] = None):
    sagemaker_delete_endpoint = SyncBotoAgentTask(
        name=f"sagemaker-delete-endpoint-{name}",
        config={"EndpointName": "{endpoint_name}"},
        service="sagemaker",
        method="delete_endpoint",
        region=region,
    )

    sagemaker_delete_endpoint_config = SyncBotoAgentTask(
        name=f"sagemaker-delete-endpoint-config-{name}",
        config={"EndpointConfigName": "{endpoint_config_name}"},
        service="sagemaker",
        method="delete_endpoint_config",
        region=region,
    )

    sagemaker_delete_model = SyncBotoAgentTask(
        name=f"sagemaker-delete-model-{name}",
        config={"ModelName": "{model_name}"},
        service="sagemaker",
        method="delete_model",
        region=region,
    )

    wf = Workflow(name=f"sagemaker-delete-endpoint-wf-{name}")
    wf.add_workflow_input("endpoint_name", str)
    wf.add_workflow_input("endpoint_config_name", str)
    wf.add_workflow_input("model_name", str)

    wf.add_entity(
        sagemaker_delete_endpoint,
        inputs=literals.LiteralMap({"endpoint_name": wf.inputs["endpoint_name"]}),
    )
    wf.add_entity(
        sagemaker_delete_endpoint_config,
        inputs=literals.LiteralMap({"endpoint_config_name": wf.inputs["endpoint_config_name"]}),
    )
    wf.add_entity(
        sagemaker_delete_model,
        inputs=literals.LiteralMap({"model_name", wf.inputs["model_name"]}),
    )

    return wf


def invoke_endpoint(
    name: str,
    config: dict[str, Any],
    output_result_type: Type,
    region: Optional[str] = None,
):
    sagemaker_invoke_endpoint = SyncBotoAgentTask(
        name=f"sagemaker-invoke-endpoint-{name}",
        config=config,
        service="sagemaker-runtime",
        method="invoke_endpoint_async",
        region=region,
    )

    wf = Workflow(name=f"sagemaker-invoke-endpoint-wf-{name}")
    wf.add_workflow_input("inputs", dict)

    invoke_node = wf.add_entity(
        sagemaker_invoke_endpoint,
        inputs=wf.inputs["inputs"],
        output_result_type=dict[str, Union[str, output_result_type]],
    )

    wf.add_workflow_output(
        "result",
        invoke_node.outputs["o0"],
        python_type=dict[str, Union[str, output_result_type]],
    )

    lp = LaunchPlan.get_or_create(workflow=wf, default_inputs={"inputs": None})
    return lp
