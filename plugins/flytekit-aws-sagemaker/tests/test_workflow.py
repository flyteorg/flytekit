from flytekitplugins.awssagemaker import (
    create_sagemaker_deployment,
    delete_sagemaker_deployment,
)
from flytekit import kwtypes


def test_sagemaker_deployment_workflow():
    sagemaker_deployment_wf = create_sagemaker_deployment(
        name="sagemaker-deployment",
        model_input_types=kwtypes(model_path=str, execution_role_arn=str),
        model_config={
            "ModelName": "sagemaker-xgboost",
            "PrimaryContainer": {
                "Image": "{container.image}",
                "ModelDataUrl": "{inputs.model_path}",
            },
            "ExecutionRoleArn": "{inputs.execution_role_arn}",
        },
        endpoint_config_input_types=kwtypes(instance_type=str),
        endpoint_config_config={
            "EndpointConfigName": "sagemaker-xgboost-endpoint-config",
            "ProductionVariants": [
                {
                    "VariantName": "variant-name-1",
                    "ModelName": "sagemaker-xgboost",
                    "InitialInstanceCount": 1,
                    "InstanceType": "{inputs.instance_type}",
                },
            ],
            "AsyncInferenceConfig": {
                "OutputConfig": {"S3OutputPath": "s3://sagemaker-agent-xgboost/inference-output/output"}
            },
        },
        endpoint_config={
            "EndpointName": "sagemaker-xgboost-endpoint",
            "EndpointConfigName": "sagemaker-xgboost-endpoint-config",
        },
        container_image="1234567890.dkr.ecr.us-east-2.amazonaws.com/sagemaker-xgboost",
        region="us-east-2",
    )

    assert len(sagemaker_deployment_wf.interface.inputs) == 3
    assert len(sagemaker_deployment_wf.interface.outputs) == 1
    assert len(sagemaker_deployment_wf.nodes) == 3


def test_sagemaker_deployment_deletion_workflow():
    sagemaker_deployment_deletion_wf = delete_sagemaker_deployment(
        name="sagemaker-deployment-deletion", region="us-east-2"
    )

    assert len(sagemaker_deployment_deletion_wf.interface.inputs) == 3
    assert len(sagemaker_deployment_deletion_wf.interface.outputs) == 0
    assert len(sagemaker_deployment_deletion_wf.nodes) == 3
