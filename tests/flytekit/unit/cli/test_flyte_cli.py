import mock as _mock
import pytest
from click.testing import CliRunner as _CliRunner

from flytekit.clis.flyte_cli import main as _main
from flytekit.common.exceptions.user import FlyteAssertion
from flytekit.common.types import primitives
from flytekit.configuration import TemporaryConfiguration
from flytekit.models import filters as _filters
from flytekit.models.admin import common as _admin_common
from flytekit.models.core import identifier as _core_identifier
from flytekit.models.project import Project as _Project
from flytekit.sdk.tasks import inputs, outputs, python_task

mm = _mock.MagicMock()
mm.return_value = 100


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


@_mock.patch("flytekit.clis.flyte_cli.main._load_proto_from_file")
def test__extract_files(load_mock):
    t = get_sample_task()
    with TemporaryConfiguration(
        "",
        internal_overrides={"image": "myflyteimage:v123", "project": "myflyteproject", "domain": "development"},
    ):
        task_spec = t.serialize()

    load_mock.side_effect = [task_spec]
    new_id, entity = _main._extract_pair("a", 1, "myproject", "development", "v", {})
    assert (
        new_id
        == _core_identifier.Identifier(
            _core_identifier.ResourceType.TASK, "myproject", "development", "test_flyte_cli.my_task", "v"
        ).to_flyte_idl()
    )
    assert task_spec == entity


@_mock.patch("flytekit.clis.flyte_cli.main._load_proto_from_file")
def test__extract_files_with_unspecified_resource_type(load_mock):
    id = _core_identifier.Identifier(
        _core_identifier.ResourceType.UNSPECIFIED,
        "myproject",
        "development",
        "name",
        "v",
    )

    load_mock.return_value = id.to_flyte_idl()
    with pytest.raises(FlyteAssertion):
        _main._extract_pair("a", "b", "myflyteproject", "development", "v", {})


def _identity_dummy(a, b, project, domain, version, patches):
    return (a, b)


@_mock.patch("flytekit.clis.flyte_cli.main._extract_pair", new=_identity_dummy)
def test__extract_files_pair_iterator():
    results = _main._extract_files("myflyteproject", "development", "v", ["1.pb", "2.pb"], None)
    assert [("1.pb", 1), ("2.pb", 2)] == results


@_mock.patch("flytekit.clis.flyte_cli.main._friendly_client.SynchronousFlyteClient")
def test_list_projects(mock_client):
    mock_client().list_projects_paginated.return_value = ([], "")
    runner = _CliRunner()
    result = runner.invoke(
        _main._flyte_cli, ["list-projects", "-h", "a.b.com", "-i", "--filter", "ne(state,-1)", "--sort-by", "asc(name)"]
    )
    assert result.exit_code == 0
    mock_client().list_projects_paginated.assert_called_with(
        limit=100,
        token="",
        filters=[_filters.Filter.from_python_std("ne(state,-1)")],
        sort_by=_admin_common.Sort.from_python_std("asc(name)"),
    )


@_mock.patch("flytekit.clis.flyte_cli.main._friendly_client.SynchronousFlyteClient")
def test_archive_project(mock_client):
    runner = _CliRunner()
    result = runner.invoke(_main._flyte_cli, ["archive-project", "-p", "foo", "-h", "a.b.com", "-i"])
    assert result.exit_code == 0
    mock_client().update_project.assert_called_with(_Project.archived_project("foo"))


@_mock.patch("flytekit.clis.flyte_cli.main._friendly_client.SynchronousFlyteClient")
def test_activate_project(mock_client):
    runner = _CliRunner()
    result = runner.invoke(_main._flyte_cli, ["activate-project", "-p", "foo", "-h", "a.b.com", "-i"])
    assert result.exit_code == 0
    mock_client().update_project.assert_called_with(_Project.active_project("foo"))


def test_flyte_cli():
    runner = _CliRunner()
    result = runner.invoke(_main._flyte_cli, ["-c", "~/.flyte/config", "activate-project", "-i"])
    assert "Config file not found at ~/.flyte/config" in result.output
    with _mock.patch("os.path.exists") as mock_exists:
        result = runner.invoke(_main._flyte_cli, ["activate-project", "-p", "foo", "-i"])
        assert "Using default config file at" in result.output
        mock_exists.return_value = True
        result = runner.invoke(_main._flyte_cli, ["-c", "~/.flyte/config", "activate-project", "-i"])
        assert "Using config file at ~/.flyte/config" in result.output
