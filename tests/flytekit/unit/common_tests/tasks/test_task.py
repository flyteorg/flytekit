from __future__ import absolute_import

import pytest as _pytest
from mock import patch as _patch, MagicMock as _MagicMock

from flytekit.common.exceptions import user as _user_exceptions
from flytekit.common.tasks import task as _task
from flytekit.models import task as _task_models
from flytekit.models.core import identifier as _identifier


@_patch("flytekit.engines.loader.get_engine")
def test_fetch_latest(mock_get_engine):
    admin_task = _task_models.Task(
        _identifier.Identifier(_identifier.ResourceType.TASK, "p1", "d1", "n1", "v1"),
        _MagicMock(),
    )
    mock_engine = _MagicMock()
    mock_engine.fetch_latest_task = _MagicMock(
        return_value=admin_task
    )
    mock_get_engine.return_value = mock_engine
    task = _task.SdkTask.fetch_latest("p1", "d1", "n1")
    assert task.id == admin_task.id


@_patch("flytekit.engines.loader.get_engine")
def test_fetch_latest_not_exist(mock_get_engine):
    mock_engine = _MagicMock()
    mock_engine.fetch_latest_task = _MagicMock(
        return_value=None
    )
    mock_get_engine.return_value = mock_engine
    with _pytest.raises(_user_exceptions.FlyteEntityNotExistException):
        _task.SdkTask.fetch_latest("p1", "d1", "n1")
