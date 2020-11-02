from flytekit.annotated.type_engine import (
    TypeEngine,
)
from flytekit.models import types as model_types
from flytekit.sdk.types import Types


def test_type_engine():
    t = Types.Integer
    lt = TypeEngine.to_literal_type(t)
    assert lt.simple == model_types.SimpleType.INTEGER
