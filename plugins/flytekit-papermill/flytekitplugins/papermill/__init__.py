"""
.. currentmodule:: flytekitplugins.papermill

This package contains things that are useful when extending Flytekit.

.. autosummary::
   :template: custom.rst
   :toctree: generated/

   NotebookTask
   record_outputs
"""

from .task import NotebookTask, read_flytedirectory, read_flytefile, read_structureddataset, record_outputs
