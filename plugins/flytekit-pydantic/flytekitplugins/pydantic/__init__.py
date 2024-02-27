import warnings

from .basemodel_transformer import BaseModelTransformer
from .deserialization import set_validators_on_supported_flyte_types as _set_validators_on_supported_flyte_types

_set_validators_on_supported_flyte_types()  # enables you to use flytekit.types in pydantic model

warnings.warn(
    "If you are using Pydantic version 2.0 or later, please import BaseModel using `from pydantic.v1 import BaseModel`.",
    FutureWarning,
)
