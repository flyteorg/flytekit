import numpy as np
import pandas as pd
import whylogs as ylog
# TODO Loads the type transformer implicitly, but should I have to?
from flytekitplugins.whylogs import WhylogsDatasetProfileTransformer 
from whylogs.core import DatasetProfileView

from flytekit import task, workflow

def test_workflow_with_whylogs():
    @task
    def make_data() -> pd.DataFrame:
        data = {'Letters': ['a', 'b', 'c', 'c'],
                'Num': [1, 2, 3, 3]}
        return pd.DataFrame(data)

    @task
    def profile(df: pd.DataFrame) -> DatasetProfileView:
        result = ylog.log(df)
        return result.profile().view()

    @workflow
    def wf() -> DatasetProfileView:
        df = make_data()
        return profile(df=df)

    profile = wf()
    keys = list(profile.get_columns().keys())
    # Sanity check that stuff is still in there
    assert keys == ['Letters', 'Num']
