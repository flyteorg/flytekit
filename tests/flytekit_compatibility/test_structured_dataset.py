import pandas as pd

from flytekit.configuration.sdk import USE_STRUCTURED_DATASET
from flytekit.core.type_engine import TypeEngine


def test_pandas_is_schema_with_flag():
    # This test can only be run iff USE_STRUCTURED_DATASET is not set
    assert USE_STRUCTURED_DATASET.get() is False

    lt = TypeEngine.to_literal_type(pd.DataFrame)
    assert lt.schema is not None
