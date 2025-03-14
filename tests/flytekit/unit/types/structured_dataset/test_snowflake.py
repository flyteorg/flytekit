import mock
import pytest
from typing_extensions import Annotated
import sys

from flytekit import StructuredDataset, kwtypes, task, workflow

try:
    import numpy as np
    numpy_installed = True
except ImportError:
    numpy_installed = False

skip_if_wrong_numpy_version = pytest.mark.skipif(
    not numpy_installed or np.__version__ > '1.26.4',
    reason="Test skipped because either NumPy is not installed or the installed version is greater than 1.26.4. "
           "Ensure that NumPy is installed and the version is <= 1.26.4, as required by the Snowflake connector."

)

@pytest.mark.skipif("pandas" not in sys.modules, reason="Pandas is not installed.")
@skip_if_wrong_numpy_version
@mock.patch("flytekit.types.structured.snowflake.get_private_key", return_value="pb")
@mock.patch("snowflake.connector.connect")
def test_sf_wf(mock_connect, mock_get_private_key):
    import pandas as pd
    from flytekit.lazy_import.lazy_module import is_imported
    from flytekit.types.structured import register_snowflake_handlers
    from flytekit.types.structured.structured_dataset import DuplicateHandlerError

    if is_imported("snowflake.connector"):
        try:
            register_snowflake_handlers()
        except DuplicateHandlerError:
            pass


    pd_df = pd.DataFrame({"Name": ["Tom", "Joseph"], "Age": [20, 22]})
    my_cols = kwtypes(Name=str, Age=int)

    @task
    def gen_df() -> Annotated[pd.DataFrame, my_cols, "parquet"]:
        return pd_df

    @task
    def t1(df: pd.DataFrame) -> Annotated[StructuredDataset, my_cols]:
        return StructuredDataset(
            dataframe=df,
            uri="snowflake://dummy_user/dummy_account/COMPUTE_WH/FLYTE/PUBLIC/TEST"
        )

    @task
    def t2(sd: Annotated[StructuredDataset, my_cols]) -> pd.DataFrame:
        return sd.open(pd.DataFrame).all()

    @workflow
    def wf() -> pd.DataFrame:
        df = gen_df()
        sd = t1(df=df)
        return t2(sd=sd)

    class mock_dataframe:
        def to_dataframe(self):
            return pd.DataFrame({"Name": ["Tom", "Joseph"], "Age": [20, 22]})

    mock_connect_instance = mock_connect.return_value
    mock_coursor_instance = mock_connect_instance.cursor.return_value
    mock_coursor_instance.fetch_pandas_all.return_value = mock_dataframe().to_dataframe()

    assert wf().equals(pd_df)
