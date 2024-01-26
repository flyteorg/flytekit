"""
.. currentmodule:: flytekitplugins.awssagemaker

.. autosummary::
   :template: custom.rst
   :toctree: generated/

   SyncBotoAgent
   SyncBotoTask
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
from .task import (
    SagemakerDeleteEndpointConfigTask,
    SagemakerDeleteEndpointTask,
    SagemakerDeleteModelTask,
    SagemakerEndpointTask,
    SagemakerEndpointConfigTask,
    SagemakerInvokeEndpointTask,
    SagemakerModelTask,
)
from .boto3_agent import SyncBotoAgent
from .boto3_task import SyncBotoTask
from .workflow import create_sagemaker_deployment, delete_sagemaker_deployment
