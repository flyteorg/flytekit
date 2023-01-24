"""
.. currentmodule:: flytekitplugins.bigquery

This package contains things that are useful when extending Flytekit.

.. autosummary::
   :template: custom.rst
   :toctree: generated/

   BigQueryConfig
   BigQueryTask
"""

from .backend_plugin import BigQueryPlugin
from .task import BigQueryConfig, BigQueryTask
