"""
.. currentmodule:: flytekitplugins.airflow

This package contains things that are useful when extending Flytekit.

.. autosummary::
   :template: custom.rst
   :toctree: generated/

   AirflowConfig
   AirflowTask
   AirflowAgent
"""
import os

if not os.getenv("FLYTE_INTERNAL_EXECUTION_ID"):
    from .agent import AirflowAgent
from .task import AirflowConfig, AirflowTask
