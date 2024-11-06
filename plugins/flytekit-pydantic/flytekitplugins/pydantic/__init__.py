from flytekit.loggers import logger

from .basemodel_transformer import BaseModelTransformer
from .deserialization import set_validators_on_supported_flyte_types as _set_validators_on_supported_flyte_types

_set_validators_on_supported_flyte_types()  # enables you to use flytekit.types in pydantic model
logger.warning(
    "The Flytekit Pydantic V1 plugin is deprecated.\n"
    "Please uninstall `flytekitplugins-pydantic` and install Pydantic directly.\n"
    "You can now use Pydantic V2 BaseModels in Flytekit tasks."
)
