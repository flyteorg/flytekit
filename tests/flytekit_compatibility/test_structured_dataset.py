import pandas as pd

from flytekit.core.type_engine import TypeEngine


def test_pandas_is_schema_with_flag():
    lt = TypeEngine.to_literal_type(pd.DataFrame)
    assert lt.schema is not None
