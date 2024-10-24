"""
.. currentmodule:: flytekitplugin.skyplane

This package provides functionality for integrating Skyplane into Flyte workflows.

.. autosummary::
   :template: custom.rst
   :toctree: generated/

   SkyplaneFunctionTask
   SkyplaneJob
"""

from .skyplane import SkyplaneFunctionTask, SkyplaneJob

__all__ = ["SkyplaneFunctionTask", "SkyplaneJob"]