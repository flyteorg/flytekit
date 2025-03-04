"""
.. currentmodule:: flytekitplugins.bigquery

This package contains things that are useful when extending Flytekit.

.. autosummary::
   :template: custom.rst
   :toctree: generated/

   BigQueryConfig
   BigQueryTask
   BigQueryAgent
"""

from .agent import BigQueryConnector
from .task import BigQueryConfig, BigQueryTask
