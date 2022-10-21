"""
Flytekit Tensorflow
=========================================
.. currentmodule:: flytekit.extras.tensorflow

.. autosummary::
   :template: custom.rst
   :toctree: generated/
"""
from flytekit.loggers import logger

# TODO: abstract this out so that there's an established pattern for registering plugins
# that have soft dependencies
try:
    # isolate the exception to the torch import
    import tensorflow

    _tensorflow_installed = True
except (ImportError, OSError):
    _tensorflow_installed = False


if _tensorflow_installed:
    from .model.native import TensorflowModelTransformer
else:
    logger.info("Unable to register TensorflowModelTransformer because tensorflow is not installed.")
