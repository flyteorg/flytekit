"""
.. currentmodule:: flytekitplugins.neptune

This package contains things that are useful when extending Flytekit.

.. autosummary::
   :template: custom.rst
   :toctree: generated/

   neptune_scale_run
"""

from .scale_tracking import neptune_scale_run

__all__ = ["neptune_scale_run"]

try:
    import neptune  # noqa: F401

    from .tracking import neptune_init_run

    __all__.append("neptune_init_run")
except ImportError:
    pass
