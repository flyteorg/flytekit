"""
.. currentmodule:: flytekitplugins.awssagemaker

.. autosummary::
   :template: custom.rst
   :toctree: generated/

   SagemakerDeleteEndpointConfigTask
   SagemakerDeleteEndpointTask
   SagemakerDeleteModelTask
   SagemakerEndpointAgent
   SagemakerEndpointConfigTask
   SagemakerInvokeEndpointTask
   SagemakerModelTask
   SyncBotoAgentTask
   SagemakerEndpointTask
   create_sagemaker_deployment
   delete_sagemaker_deployment
"""

from .agent import (
    SagemakerDeleteEndpointConfigTask,
    SagemakerDeleteEndpointTask,
    SagemakerDeleteModelTask,
    SagemakerEndpointAgent,
    SagemakerEndpointConfigTask,
    SagemakerInvokeEndpointTask,
    SagemakerModelTask,
)
from .boto3.agent import SyncBotoAgentTask
from .task import SagemakerEndpointTask
from .workflow import create_sagemaker_deployment, delete_sagemaker_deployment
