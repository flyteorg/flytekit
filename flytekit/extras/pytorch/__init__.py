"""
Flytekit PyTorch
=========================================
.. currentmodule:: flytekit.extras.pytorch

.. autosummary::
   :template: custom.rst
   :toctree: generated/

    PyTorchCheckpoint
"""
from flytekit.loggers import logger

try:
    from .checkpoint import PyTorchCheckpoint, PyTorchCheckpointTransformer
    from .native import PyTorchModuleTransformer, PyTorchTensorTransformer
except ImportError:
    logger.info(
        "We won't register PyTorchCheckpointTransformer, PyTorchTensorTransformer, and PyTorchModuleTransformer because torch is not installed."
    )
