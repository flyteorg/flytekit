from datetime import datetime

import pandas as pd
import pytest
import whylogs as why
from whylogs.core import DatasetProfileView

from flytekit import task, workflow


@pytest.fixture
def input_data():
    return pd.DataFrame({"a": [1, 2, 3, 4]})


@task
def whylogs_profiling(data: pd.DataFrame) -> DatasetProfileView:
    result = why.log(pandas=data)
    return result.view()


@task
def fetch_whylogs_datetime(profile_view: DatasetProfileView) -> datetime:
    return profile_view.dataset_timestamp


@workflow
def whylogs_wf(data: pd.DataFrame) -> datetime:
    profile_view = whylogs_profiling(data=data)
    return fetch_whylogs_datetime(profile_view=profile_view)


def test_task_returns_whylogs_profile_view(input_data):
    actual_profile = whylogs_profiling(data=input_data)
    assert actual_profile is not None
    assert isinstance(actual_profile, DatasetProfileView)


def test_profile_view_gets_passed_on_tasks(input_data):
    result = whylogs_wf(data=input_data)
    assert result is not None
    assert isinstance(result, datetime)
