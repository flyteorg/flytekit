import mock as _mock
import pytest
from click.testing import CliRunner as _CliRunner

from flytekit.clis.flyte_cli import main as _main
from flytekit.common.exceptions.user import FlyteAssertion
from flytekit.common.types import primitives
from flytekit.configuration import TemporaryConfiguration
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
    id = _core_identifier.Identifier(_core_identifier.ResourceType.TASK, "myproject", "development", "name", "v")
    t = get_sample_task()
    with TemporaryConfiguration(
        "", internal_overrides={"image": "myflyteimage:v123", "project": "myflyteproject", "domain": "development"},
    ):
        task_spec = t.serialize()

    load_mock.side_effect = [id.to_flyte_idl(), task_spec]
    new_id, entity = _main._extract_pair("a", "b")
    assert new_id == id.to_flyte_idl()
    assert task_spec == entity


@_mock.patch("flytekit.clis.flyte_cli.main._load_proto_from_file")
def test__extract_files_with_unspecified_resource_type(load_mock):
    id = _core_identifier.Identifier(
        _core_identifier.ResourceType.UNSPECIFIED, "myproject", "development", "name", "v",
    )

    load_mock.return_value = id.to_flyte_idl()
    with pytest.raises(FlyteAssertion):
        _main._extract_pair("a", "b")


def _identity_dummy(a, b):
    return (a, b)


@_mock.patch("flytekit.clis.flyte_cli.main._extract_pair", new=_identity_dummy)
def test__extract_files_pair_iterator():
    results = _main._extract_files([1, 2, 3, 4])
    assert [(1, 2), (3, 4)] == results


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
