"""
.. currentmodule:: flytekitplugins.airflow

This package contains things that are useful when extending Flytekit.

.. autosummary::
   :template: custom.rst
   :toctree: generated/

   AirflowConfig
   AirflowTask
   AirflowConnector
"""

from .connector import AirflowConnector
from .task import AirflowObj, AirflowTask
