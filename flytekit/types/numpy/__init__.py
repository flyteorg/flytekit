from flytekit.loggers import logger

try:
    # isolate the exception to the numpy import
    import numpy

    _numpy_installed = True
except ImportError:
    _numpy_installed = False


if _numpy_installed:
    from .ndarray import NumpyArrayTransformer
else:
    logger.info("We won't register NumpyArrayTransformer because numpy is not installed.")
