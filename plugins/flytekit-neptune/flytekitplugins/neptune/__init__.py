"""
.. currentmodule:: flytekitplugins.neptune

This package contains things that are useful when extending Flytekit.

.. autosummary::
   :template: custom.rst
   :toctree: generated/

   neptune_init_run
"""

import warnings

from .tracking import neptune_init_run

warnings.warn(
    "The `flytekitplugins-neptune` plugin is deprecated. Use `flytekitplugins-neptune-scale` instead.",
    DeprecationWarning,
    stacklevel=2,
)

__all__ = ["neptune_init_run"]
