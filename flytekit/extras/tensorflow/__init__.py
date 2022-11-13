"""
Flytekit Tensorflow
=========================================
.. currentmodule:: flytekit.extras.tensorflow

.. autosummary::
   :template: custom.rst
   :toctree: generated/

    TensorflowRecord
"""
from flytekit.loggers import logger

# TODO: abstract this out so that there's an established pattern for registering plugins
# that have soft dependencies
try:
    # isolate the exception to the tensorflow import
    import tensorflow

    _tensorflow_installed = True
except (ImportError, OSError):
    _tensorflow_installed = False


if _tensorflow_installed:
    from .records import TensorflowExampleRecordsTransformer
else:
    logger.info("We won't register TensorflowRecordsTransformer because tensorflow is not installed.")
