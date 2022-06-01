"""
Flytekit PyTorch
=========================================
.. currentmodule:: flytekit.types.pytorch

.. autosummary::
   :template: custom.rst
   :toctree: generated/

    PyTorchTensorTransformer
    PyTorchStateDict
    PyTorchModuleTransformer
"""
from flytekit.loggers import logger

try:
    from .module import PyTorchModuleTransformer, PyTorchStateDict
    from .tensor import PyTorchTensorTransformer
except ImportError:
    logger.info(
        "We won't register PyTorchTensorTransformer and PyTorchModuleTransformer because torch is not installed."
    )
