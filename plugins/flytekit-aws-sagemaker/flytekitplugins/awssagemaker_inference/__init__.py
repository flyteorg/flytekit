"""
.. currentmodule:: flytekitplugins.awssagemaker_inference

.. autosummary::
   :template: custom.rst
   :toctree: generated/

   BotoConnector
   BotoTask
   SageMakerModelTask
   SageMakerEndpointConfigTask
   SageMakerEndpointConnector
   SageMakerEndpointTask
   SageMakerDeleteEndpointConfigTask
   SageMakerDeleteEndpointTask
   SageMakerDeleteModelTask
   SageMakerInvokeEndpointTask
   create_sagemaker_deployment
   delete_sagemaker_deployment
"""

from .boto3_connector import BotoConnector
from .boto3_task import BotoConfig, BotoTask
from .connector import SageMakerEndpointConnector
from .task import (
    SageMakerDeleteEndpointConfigTask,
    SageMakerDeleteEndpointTask,
    SageMakerDeleteModelTask,
    SageMakerEndpointConfigTask,
    SageMakerEndpointTask,
    SageMakerInvokeEndpointTask,
    SageMakerModelTask,
)
from .workflow import create_sagemaker_deployment, delete_sagemaker_deployment


def triton_image_uri(version: str = "23.12"):
    image = "{account_id}.dkr.ecr.{region}.{base}/sagemaker-tritonserver:{version}-py3"
    return image.replace("{version}", version)
