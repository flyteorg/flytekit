"""
==========
ImageSpec
==========

.. currentmodule:: flytekit.image_spec

This module contains the ImageSpec class parameters and methods.

.. autosummary::
   :nosignatures:
   :template: custom.rst
   :toctree: generated/

   ImageSpec
"""

from .default_builder import DefaultImageBuilder
from .image_spec import ImageBuildEngine, ImageSpec
from .noop_builder import NoOpBuilder

# Set this to a lower priority compared to `envd` to maintain backward compatibility
ImageBuildEngine.register(DefaultImageBuilder.builder_type, DefaultImageBuilder(), priority=1)
# Lower priority compared to Default.
ImageBuildEngine.register(NoOpBuilder.builder_type, NoOpBuilder(), priority=0)
