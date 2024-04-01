"""
.. currentmodule:: flytekitplugins.awssagemaker_inference

.. autosummary::
   :template: custom.rst
   :toctree: generated/

   BotoAgent
   BotoTask
   SageMakerModelTask
   SageMakerEndpointConfigTask
   SageMakerEndpointAgent
   SageMakerEndpointTask
   SageMakerDeleteEndpointConfigTask
   SageMakerDeleteEndpointTask
   SageMakerDeleteModelTask
   SageMakerInvokeEndpointTask
   create_sagemaker_deployment
   delete_sagemaker_deployment
"""

from functools import partial

from .agent import SageMakerEndpointAgent
from .boto3_agent import BotoAgent
from .boto3_task import BotoConfig, BotoTask
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
    return partial(
        "{account_id}.dkr.ecr.{region}.{base}/sagemaker-tritonserver:{version}-py3".format,
        version=version,
    )
