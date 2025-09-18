"""
.. currentmodule:: flytekitplugins.dgxc_lepton

This package contains components for integrating Flyte with Lepton AI.

.. autosummary::
   :template: custom.rst
   :toctree: generated/

   BaseLeptonConfig
   LeptonEndpointConfig
   LeptonConfig
   BaseLeptonTask
   LeptonEndpointTask
   LeptonTask
   LeptonEndpointConnector
   LeptonConnector
"""

from .connector import LeptonConnector, LeptonEndpointConnector
from .task import (
    BaseLeptonConfig,
    BaseLeptonTask,
    LeptonConfig,  # Backward compatibility
    LeptonEndpointConfig,
    LeptonEndpointTask,
    LeptonTask,  # Backward compatibility
)

# Main exports for current usage
__all__ = [
    # Current usage (backward compatible)
    "LeptonConfig",
    "LeptonTask",
    "LeptonConnector",
    # New modular structure (for future extensions)
    "BaseLeptonConfig",
    "LeptonEndpointConfig",
    "BaseLeptonTask",
    "LeptonEndpointTask",
    "LeptonEndpointConnector",
]
