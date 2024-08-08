import datetime
import os

import mock
import pytest
from click.testing import CliRunner

from flytekit.clis.sdk_in_container import pyflyte
from flytekit.models import task as _task
from flytekit.models.core.identifier import Identifier as _identifier
from flytekit.models.core.identifier import ResourceType as _resource_type
from flytekit.remote import FlyteTask

pytest.importorskip("pandas")

WORKFLOW_FILE = os.path.join(os.path.dirname(os.path.realpath(__file__)), "workflow.py")
REMOTE_WORKFLOW_FILE = "https://raw.githubusercontent.com/flyteorg/flytesnacks/8337b64b33df046b2f6e4cba03c74b7bdc0c4fb1/cookbook/core/flyte_basics/basic_workflow.py"
IMPERATIVE_WORKFLOW_FILE = os.path.join(os.path.dirname(os.path.realpath(__file__)), "imperative_wf.py")
DIR_NAME = os.path.dirname(os.path.realpath(__file__))


@mock.patch("flytekit.clis.sdk_in_container.versions.DynamicEntityVersionCommand._fetch_entity")
def test_pyflyte_version(mock_entity):
    runner = CliRunner()
    mock_entity.return_value = mock.MagicMock(spec=FlyteTask)

    created_at = datetime.datetime(2021, 1, 1)
    mock_closure = _task.TaskClosure(mock.MagicMock(spec=_task.CompiledTask), created_at=created_at)
    mock_tasks = [
        _task.Task(id=_identifier(_resource_type.TASK, "p1", "d1", "my_task", "my_version"), closure=mock_closure)
    ]

    with mock.patch("flytekit.clients.friendly.SynchronousFlyteClient.list_tasks_paginated") as mock_list_tasks:
        mock_list_tasks.return_value = (mock_tasks, None)
        result = runner.invoke(pyflyte.main, ["show-versions", "remote-task", "any_task"], catch_exceptions=False)

    assert "my_version" in result.output
    assert created_at.strftime("%Y-%m-%d %H:%M:%S") in result.output
    assert result.exit_code == 0


def test_pyflyte_version_no_workflows():
    with mock.patch("flytekit.configuration.plugin.FlyteRemote"):
        runner = CliRunner()
        result = runner.invoke(pyflyte.main, ["show-versions", "remote-workflow"], catch_exceptions=False)

    assert result.exit_code == 0
