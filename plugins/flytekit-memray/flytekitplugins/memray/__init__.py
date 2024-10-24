"""
.. currentmodule:: flytekitplugins.wandb

This package contains things that are useful when extending Flytekit.

.. autosummary::
   :template: custom.rst
   :toctree: generated/

   wandb_init
"""

from .profiling import mem_profiling

__all__ = ["mem_profiling"]
