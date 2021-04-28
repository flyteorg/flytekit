from mock import MagicMock as _MagicMock
from mock import patch as _patch

from flytekit.control_plane.tasks import task as _task
from flytekit.models import task as _task_models
from flytekit.models.core import identifier as _identifier


@_patch("flytekit.engines.flyte.engine._FlyteClientManager")
@_patch("flytekit.configuration.platform.URL")
def test_flyte_task_fetch(mock_url, mock_client_manager):
    mock_url.get.return_value = "localhost"
    admin_task_v1 = _task_models.Task(
        _identifier.Identifier(_identifier.ResourceType.TASK, "p1", "d1", "n1", "v1"),
        _MagicMock(),
    )
    admin_task_v2 = _task_models.Task(
        _identifier.Identifier(_identifier.ResourceType.TASK, "p1", "d1", "n1", "v2"),
        _MagicMock(),
    )
    mock_client = _MagicMock()
    mock_client.list_tasks_paginated = _MagicMock(return_value=([admin_task_v2, admin_task_v1], ""))
    mock_client_manager.return_value.client = mock_client

    latest_task = _task.FlyteTask.fetch_latest("p1", "d1", "n1")
    task_v1 = _task.FlyteTask.fetch("p1", "d1", "n1", "v1")
    task_v2 = _task.FlyteTask.fetch("p1", "d1", "n1", "v2")
    assert task_v1.id == admin_task_v1.id
    assert task_v1.id != latest_task.id
    assert task_v2.id == latest_task.id == admin_task_v2.id

    for task in [task_v1, task_v2]:
        assert task.entity_type_text == "Task"
        assert task.resource_type == _identifier.ResourceType.TASK
