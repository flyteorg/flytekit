"""
This module provides plugin classes related to AWS Athena configuration.

.. currentmodule:: flytekitplugins.athena

.. autosummary::

   AthenaConfig
   AthenaTask
"""

from .task import AthenaConfig, AthenaTask

__all__ = ["AthenaConfig", "AthenaTask"]
