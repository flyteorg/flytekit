"""
.. currentmodule:: flytekitplugins.awssagemaker_inference_recommender

.. autosummary::
   :template: custom.rst
   :toctree: generated/

   SageMakerInferenceRecommenderJobConnector
   SageMakerInferenceRecommenderJobTask
   SageMakerStopInferenceRecommenderJobTask
   SageMakerDescribeInferenceRecommenderJobTask
"""

from .connector import (
    SageMakerInferenceRecommenderJobConnector,
    SageMakerInferenceRecommenderJobMetadata,
)
from .task import (
    SageMakerDescribeInferenceRecommenderJobTask,
    SageMakerInferenceRecommenderJobTask,
    SageMakerStopInferenceRecommenderJobTask,
)

__all__ = [
    "SageMakerInferenceRecommenderJobConnector",
    "SageMakerInferenceRecommenderJobMetadata",
    "SageMakerInferenceRecommenderJobTask",
    "SageMakerStopInferenceRecommenderJobTask",
    "SageMakerDescribeInferenceRecommenderJobTask",
]
