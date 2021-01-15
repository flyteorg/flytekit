import os as _os

import pytest as _pytest
from flyteidl.admin import task_pb2 as _admin_task_pb2
from mock import MagicMock as _MagicMock
from mock import patch as _patch

import flytekit.platform.sdk_task
from flytekit.common.exceptions import user as _user_exceptions
from flytekit.legacy.tasks.presto_task import SdkPrestoTask
from flytekit.common.types import primitives
from flytekit.configuration import TemporaryConfiguration
from flytekit.models import task as _task_models
from flytekit.models.core import identifier as _identifier
from flytekit.legacy.sdk.tasks import inputs, outputs, python_task
from flytekit.legacy.sdk import Types


@_patch("flytekit.engines.flyte.engine._FlyteClientManager")
@_patch("flytekit.configuration.platform.URL")
def test_fetch_latest(mock_url, mock_client_manager):
    mock_url.get.return_value = "localhost"
    admin_task = _task_models.Task(
        _identifier.Identifier(_identifier.ResourceType.TASK, "p1", "d1", "n1", "v1"), _MagicMock(),
    )
    mock_client = _MagicMock()
    mock_client.list_tasks_paginated = _MagicMock(return_value=([admin_task], ""))
    mock_client_manager.return_value.client = mock_client
    task = flytekit.platform.sdk_task.SdkTask.fetch_latest("p1", "d1", "n1")
    assert task.id == admin_task.id


@_patch("flytekit.engines.flyte.engine._FlyteClientManager")
@_patch("flytekit.configuration.platform.URL")
def test_fetch_latest_not_exist(mock_url, mock_client_manager):
    mock_client = _MagicMock()
    mock_client.list_tasks_paginated = _MagicMock(return_value=(None, ""))
    mock_client_manager.return_value.client = mock_client
    mock_url.get.return_value = "localhost"
    with _pytest.raises(_user_exceptions.FlyteEntityNotExistException):
        flytekit.platform.sdk_task.SdkTask.fetch_latest("p1", "d1", "n1")


def get_sample_task():
    """
    :rtype: flytekit.common.tasks.task.SdkTask
    """

    @inputs(a=primitives.Integer)
    @outputs(b=primitives.Integer)
    @python_task()
    def my_task(wf_params, a, b):
        b.set(a + 1)

    return my_task


def test_task_serialization():
    t = get_sample_task()
    with TemporaryConfiguration(
        _os.path.join(_os.path.dirname(_os.path.realpath(__file__)), "../../../common/configs/local.config",),
        internal_overrides={"image": "myflyteimage:v123", "project": "myflyteproject", "domain": "development"},
    ):
        s = t.serialize()

    assert isinstance(s, _admin_task_pb2.TaskSpec)
    assert s.template.id.name == "tests.flytekit.unit.common_tests.tasks.test_task.my_task"
    assert s.template.container.image == "myflyteimage:v123"


schema = Types.Schema([("a", Types.String), ("b", Types.Integer)])


def test_task_produce_deterministic_version():
    containerless_task = SdkPrestoTask(
        task_inputs=inputs(ds=Types.String, rg=Types.String),
        statement="SELECT * FROM flyte.widgets WHERE ds = '{{ .Inputs.ds}}' LIMIT 10",
        output_schema=schema,
        routing_group="{{ .Inputs.rg }}",
    )
    identical_containerless_task = SdkPrestoTask(
        task_inputs=inputs(ds=Types.String, rg=Types.String),
        statement="SELECT * FROM flyte.widgets WHERE ds = '{{ .Inputs.ds}}' LIMIT 10",
        output_schema=schema,
        routing_group="{{ .Inputs.rg }}",
    )
    different_containerless_task = SdkPrestoTask(
        task_inputs=inputs(ds=Types.String, rg=Types.String),
        statement="SELECT * FROM flyte.widgets WHERE ds = '{{ .Inputs.ds}}' LIMIT 100000",
        output_schema=schema,
        routing_group="{{ .Inputs.rg }}",
    )
    assert (
        containerless_task._produce_deterministic_version()
        == identical_containerless_task._produce_deterministic_version()
    )

    assert (
        containerless_task._produce_deterministic_version()
        != different_containerless_task._produce_deterministic_version()
    )

    with _pytest.raises(Exception):
        get_sample_task()._produce_deterministic_version()
