import datetime as _datetime
from typing import Dict

from flytekit.common.types import primitives as _primitives
from flytekit.models import types as _type_models

# This is now in three different places. This here, the one that the notebook task uses, and the main in that meshes
# with the engine loader. I think we should get rid of the loader (can add back when/if we ever have more than one).
# All three should be merged into the existing one.
SIMPLE_TYPE_LOOKUP_TABLE: Dict[type, _type_models.LiteralType] = {
    int: _primitives.Integer.to_flyte_literal_type(),
    float: _primitives.Float.to_flyte_literal_type(),
    bool: _primitives.Boolean,
    _datetime.datetime: _primitives.Datetime.to_flyte_literal_type(),
    _datetime.timedelta: _primitives.Timedelta.to_flyte_literal_type(),
    str: _primitives.String.to_flyte_literal_type(),
    dict: _primitives.Generic.to_flyte_literal_type(),
}
