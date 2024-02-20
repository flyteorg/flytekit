"""
.. currentmodule:: flytekitplugins.awssagemaker

.. autosummary::
   :template: custom.rst
   :toctree: generated/

   BotoAgent
   BotoTask
   SagemakerModelTask
   SagemakerEndpointConfigTask
   SagemakerEndpointAgent
   SagemakerEndpointTask
   SagemakerDeleteEndpointConfigTask
   SagemakerDeleteEndpointTask
   SagemakerDeleteModelTask
   SagemakerInvokeEndpointTask
   create_sagemaker_deployment
   delete_sagemaker_deployment
"""

from .agent import SagemakerEndpointAgent
from .boto3_agent import BotoAgent
from .boto3_task import BotoConfig, BotoTask
from .task import (
    SagemakerDeleteEndpointConfigTask,
    SagemakerDeleteEndpointTask,
    SagemakerDeleteModelTask,
    SagemakerEndpointConfigTask,
    SagemakerEndpointTask,
    SagemakerInvokeEndpointTask,
    SagemakerModelTask,
)
from .workflow import create_sagemaker_deployment, delete_sagemaker_deployment
