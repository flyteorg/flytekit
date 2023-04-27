"""
.. currentmodule:: flytekitplugins.papermill

This package contains things that are useful when extending Flytekit.

.. autosummary::
   :template: custom.rst
   :toctree: generated/

   NotebookTask
   record_outputs
"""

from .task import NotebookTask, read_input, record_outputs
