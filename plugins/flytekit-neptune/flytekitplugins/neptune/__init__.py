"""
.. currentmodule:: flytekitplugins.neptune

This package contains things that are useful when extending Flytekit.

.. autosummary::
   :template: custom.rst
   :toctree: generated/

   neptune_init_run
"""

from .tracking import neptune_init_run

__all__ = ["neptune_init_run"]
