"""
====================
Papermill
====================
Papermill plugin

.. currentmodule:: flytekitplugins.papermill

.. autosummary::
    :template: custom.rst

   NotebookTask
   record_outputs
"""

from .task import NotebookTask, record_outputs

__all__ = ["NotebookTask", "record_outputs"]
