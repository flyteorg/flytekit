"""
This module provides functionality related to IMageSPec
"""

from .default_builder import DefaultImageBuilder
from .image_spec import ImageBuildEngine, ImageSpec

# Set this to a lower priority compared to `envd` to maintain backward compatibility
ImageBuildEngine.register(DefaultImageBuilder.builder_type, DefaultImageBuilder(), priority=1)
