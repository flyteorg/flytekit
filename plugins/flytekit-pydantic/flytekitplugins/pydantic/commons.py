import builtins
import datetime
import typing

from typing_extensions import Annotated

import pandas as pd


from flytekit.core import type_engine

MODULES_TO_EXLCLUDE_FROM_FLYTE_TYPES = {m.__name__ for m in [builtins, typing, datetime]}


def include_in_flyte_types(t: type) -> bool:
    if t is None:
        return False
    if t.__module__ in MODULES_TO_EXLCLUDE_FROM_FLYTE_TYPES:
        return False
    return True


PYDANTIC_SUPPORTED_FLYTE_TYPES = tuple(
    filter(include_in_flyte_types, type_engine.TypeEngine.get_available_transformers())
) + (pd.DataFrame,)

# this is the UUID placeholder that is set in the serialized basemodel JSON, connecting that field to
# the literal map that holds the actual object that needs to be deserialized (w/ protobuf)
LiteralObjID = Annotated[str, "Key for unique object in literal map."]
