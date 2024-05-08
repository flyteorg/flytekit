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


from .agent import InfluxDBAgent
from .task import InfluxDBTask
from .utils import Aggregation, influx_json_to_df
