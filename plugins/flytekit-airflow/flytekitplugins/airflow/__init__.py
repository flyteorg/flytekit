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

import os

# Configure Airflow to enable timer unit consistency to suppress deprecation warnings
os.environ.setdefault("AIRFLOW__METRICS__TIMER_UNIT_CONSISTENCY", "True")

from .connector import AirflowConnector
from .task import AirflowObj, AirflowTask
