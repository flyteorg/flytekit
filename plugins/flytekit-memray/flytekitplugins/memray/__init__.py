"""
.. currentmodule:: flytekitplugins.wandb

This package contains things that are useful when extending Flytekit.

.. autosummary::
   :template: custom.rst
   :toctree: generated/

   wandb_init
"""

from .profiling import memray_profiling

__all__ = ["memray_profiling"]
