from flytekit import Workflow, kwtypes
from .agents.sagemaker_deploy_agents import (
    SagemakerModelTask,
    SagemakerEndpointConfigTask,
)
from .task import SagemakerEndpointTask
from typing import Any, Optional


def create_sagemaker_deployment(
    model_name: str,
    region: str,
    model_config: dict[str, Any],
    endpoint_config_config: dict[str, Any],
    endpoint_config: dict[str, Any],
    model_additional_args: Optional[dict[str, Any]] = None,
    endpoint_config_additional_args: Optional[dict[str, Any]] = None,
):
    sagemaker_model_task = SagemakerModelTask(
        name=f"sagemaker-model-{model_name}",
        config=model_config,
        region=region,
    )

    endpoint_config_task = SagemakerEndpointConfigTask(
        name=f"sagemaker-endpoint-config-{model_name}",
        config=endpoint_config_config,
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
        inputs=wf.inputs["model_inputs"],
        additional_args=model_additional_args,
    )

    wf.add_entity(
        endpoint_config_task,
        inputs=wf.inputs["endpoint_config_inputs"],
        additional_args=endpoint_config_additional_args,
    )

    wf.add_entity(endpoint_task, inputs=wf.inputs["endpoint_inputs"])

    return wf
