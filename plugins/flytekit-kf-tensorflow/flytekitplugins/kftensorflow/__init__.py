"""
====================
Tensorflow
====================
Tensorflow plugin

.. currentmodule:: flytekitplugins.tensorflowjob

.. autosummary::

   TfJob
   TensorflowFunctionTask
"""

from .task import TensorflowFunctionTask, TfJob

__all__ = ["TfJob", "TensorflowFunctionTask"]
