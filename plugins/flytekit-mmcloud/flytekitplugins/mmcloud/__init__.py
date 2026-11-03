"""
.. currentmodule:: flytekitplugins.mmcloud

This package contains things that are useful when extending Flytekit.

.. autosummary::
   :template: custom.rst
   :toctree: generated/

   MMCloudConfig
   MMCloudTask
   MMCloudConnector
"""

from .connector import MMCloudConnector
from .task import MMCloudConfig, MMCloudTask
