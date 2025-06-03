"""
.. currentmodule:: flytekitplugins.neptune_scale

This package contains things that are useful when extending Flytekit.

.. autosummary::
   :template: custom.rst
   :toctree: generated/

   neptune_run
"""

from .tracking import neptune_run

__all__ = ["neptune_run"]
