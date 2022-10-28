"""
Flytekit TensorFlow
=========================================
.. currentmodule:: flytekit.extras.tensorflow

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
    from .layer import TensorFlowLayerTransformer
    from .model import TensorFlowModelTransformer, TensorFlowSequentialTransformer
else:
    logger.info(
        "Unable to register TensorFlowModelTransformer, TensorFlowSequentialTransformer, TensorFlowLayerTransformer because tensorflow is not installed."
    )
