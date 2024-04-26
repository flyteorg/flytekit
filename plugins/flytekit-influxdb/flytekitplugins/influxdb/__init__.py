"""
.. currentmodule:: flytekitplugins.influxdb

This package contains things that are useful when extending Flytekit.

.. autosummary::
   :template: custom.rst
   :toctree: generated/

   BigQueryConfig
   BigQueryTask
   BigQueryAgent
"""
from enum import Enum


from .agent import InfluxDBAgent
from .task import InfluxDBTask

DATETIME_FORMAT = "%Y-%m-%dT%H:%M:%SZ"

class Aggregation(Enum):
    """Aggregation types."""
    FIRST = "first"
    LAST = "last"
    MEAN = "mean"
    MODE = "mode"
