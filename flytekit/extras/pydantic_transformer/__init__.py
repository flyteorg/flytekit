from flytekit.loggers import logger

try:
    # isolate the exception to the pydantic import
    # model_validator and model_serializer are only available in pydantic > 2
    from pydantic import model_serializer, model_validator

    from . import transformer
except (ImportError, OSError) as e:
    logger.debug(f"Meet error when importing pydantic: `{e}`")
    logger.debug("Flytekit only support pydantic version > 2.")
