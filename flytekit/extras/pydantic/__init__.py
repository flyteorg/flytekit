from flytekit.extras.pydantic import custom
from flytekit.loggers import logger

# TODO: abstract this out so that there's an established pattern for registering plugins
# that have soft dependencies
try:
    # isolate the exception to the pydantic import
    # model_validator and model_serializer are only available in pydantic > 2
    from pydantic import model_validator, model_serializer

    _pydantic_installed = True
except (ImportError, OSError):
    _torch_ins_pydantic_installedtalled = False


if _pydantic_installed:
    from . import custom, transformer
else:
    logger.info("Flytekit only support pydantic version > 2.")
