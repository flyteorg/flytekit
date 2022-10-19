"""
Flytekit Keras
=========================================
.. currentmodule:: flytekit.extras.keras

.. autosummary::
   :template: custom.rst
   :toctree: generated/

"""
from flytekit.loggers import logger

# that have soft dependencies
try:
    # isolate the exception to the keras import
    from tensorflow import keras

    _keras_installed = True
except (ImportError, OSError):
    _keras_installed = False


if _keras_installed:
    from .native import KerasModelTransformer, KerasSequentialTransformer
else:
    logger.info(
        "We won't register KerasSequentialTransformer and KerasModelTransformer because keras is not installed."
    )
