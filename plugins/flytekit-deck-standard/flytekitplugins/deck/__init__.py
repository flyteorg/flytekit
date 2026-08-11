"""
.. currentmodule:: flytekitplugins.deck

This package contains things that are useful when extending Flytekit.

.. autosummary::
   :template: custom.rst
   :toctree: generated/

   BoxRenderer
   FrameProfilingRenderer
   GanttChartRenderer
   ImageRenderer
   MarkdownRenderer
   SourceCodeRenderer
   TableRenderer
   PyTorchProfilingRenderer
"""

from .renderer import (
    BoxRenderer,
    FrameProfilingRenderer,
    GanttChartRenderer,
    ImageRenderer,
    MarkdownRenderer,
    SourceCodeRenderer,
    TableRenderer,
    PyTorchProfilingRenderer
)

__all__ = [
    "BoxRenderer",
    "FrameProfilingRenderer",
    "GanttChartRenderer",
    "ImageRenderer",
    "MarkdownRenderer",
    "SourceCodeRenderer",
    "TableRenderer",
    "PyTorchProfilingRenderer"  
]
