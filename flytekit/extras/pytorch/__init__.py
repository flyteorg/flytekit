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

# TODO: abstract this out so that there's an established pattern for registering plugins
# that have soft dependencies
try:
    from .checkpoint import PyTorchCheckpoint, PyTorchCheckpointTransformer
    from .native import PyTorchModuleTransformer, PyTorchTensorTransformer
except (ImportError, OSError):
    logger.info(
        "We won't register PyTorchCheckpointTransformer, PyTorchTensorTransformer, and PyTorchModuleTransformer because torch is not installed."
    )
