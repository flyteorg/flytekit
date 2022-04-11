"""
====================
AWS Athena plugin
====================
AWS Athena plugin

.. currentmodule:: flytekitplugins.athena

.. autosummary::
    :template: custom.rst

   AthenaConfig
   AthenaTask
"""

from .task import AthenaConfig, AthenaTask

__all__ = ["AthenaConfig", "AthenaTask"]
