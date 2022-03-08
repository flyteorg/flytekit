from datetime import datetime, timedelta

import pytest

from flytekit import kwtypes
from flytekit.core import context_manager
from flytekit.core.context_manager import ExecutionState
from flytekit.core.type_engine import TypeEngine
from flytekit.types.schema import FlyteSchema, SchemaFormat
from flytekit.types.schema.types import FlyteSchemaTransformer


def test_typed_schema():
    s = FlyteSchema[kwtypes(x=int, y=float)]
    assert s.format() == SchemaFormat.PARQUET
    assert s.columns() == {"x": int, "y": float}


def test_assert_type():
    ctx = context_manager.FlyteContextManager.current_context()
    with context_manager.FlyteContextManager.with_context(
        ctx.with_execution_state(ctx.new_execution_state().with_params(mode=ExecutionState.Mode.TASK_EXECUTION))
    ) as ctx:
        schema = FlyteSchema[kwtypes(x=int, y=float)]
        fst = FlyteSchemaTransformer()
        lt = fst.get_literal_type(schema)
        with pytest.raises(ValueError, match="DataFrames of type <class 'int'> are not supported currently"):
            TypeEngine.to_literal(ctx, 3, schema, lt)


def test_schema_back_and_forth():
    orig = FlyteSchema[kwtypes(TrackId=int, Name=str)]
    lt = TypeEngine.to_literal_type(orig)
    pt = TypeEngine.guess_python_type(lt)
    lt2 = TypeEngine.to_literal_type(pt)
    assert lt == lt2


def test_remaining_prims():
    orig = FlyteSchema[kwtypes(my_dt=datetime, my_td=timedelta, my_b=bool)]
    lt = TypeEngine.to_literal_type(orig)
    pt = TypeEngine.guess_python_type(lt)
    lt2 = TypeEngine.to_literal_type(pt)
    assert lt == lt2


def test_bad_conversion():
    orig = FlyteSchema[kwtypes(my_custom=bool)]
    lt = TypeEngine.to_literal_type(orig)
    # Make a not real column type
    lt.schema.columns[0]._type = 15
    with pytest.raises(ValueError):
        TypeEngine.guess_python_type(lt)
