from flytekit.loggers import logger

# TODO: abstract this out so that there's an established pattern for registering plugins
# that have soft dependencies
try:
    # isolate the exception to the pydantic import
    # model_validator and model_serializer are only available in pydantic > 2
    from pydantic import model_serializer, model_validator
    from . import custom, transformer
except (ImportError, OSError) as e:
    logger.info(f"Meet error when importing pydantic: `{e}`")
    logger.info("Flytekit only support pydantic version > 2.")
