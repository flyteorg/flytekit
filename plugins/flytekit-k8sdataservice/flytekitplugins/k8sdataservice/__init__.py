"""
.. currentmodule:: flytekitplugins.k8sdataservice

This package contains things that are useful when extending Flytekit.

.. autosummary::
   :template: custom.rst
   :toctree: generated/

   DataServiceTask
"""

from .connector import DataServiceConnector  # noqa: F401
from .sensor import CleanupSensor  # noqa: F401
from .task import DataServiceConfig, DataServiceTask  # noqa: F401
