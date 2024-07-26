"""
.. currentmodule:: flytekitplugins.ray

This package contains things that are useful when extending Flytekit.

.. autosummary::
   :template: custom.rst
   :toctree: generated/

   HeadNodeConfig
   RayJobConfig
   WorkerNodeConfig
"""

from .agent import AnyscaleAgent
from .task import HeadNodeConfig, RayJobConfig, WorkerNodeConfig
