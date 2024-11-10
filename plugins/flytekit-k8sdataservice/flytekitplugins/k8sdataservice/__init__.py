"""
.. currentmodule:: flytekitplugins.k8sdataservice

This package contains things that are useful when extending Flytekit.

.. autosummary::
   :template: custom.rst
   :toctree: generated/

   DataServiceTask
"""

from .agent import DataServiceAgent  # noqa: F401
from .task import DataServiceTask, DataServiceConfig  # noqa: F401
from .sensor import CleanupSensor  # noqa: F401
# from .dataservice_sensor_engine import DSSensorEngine  # noqa: F401
