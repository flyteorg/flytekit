import mock as _mock

from flytekit.clients.friendly import SynchronousFlyteClient as _SynchronousFlyteClient
from flytekit.models.project import Project as _Project


@_mock.patch("flytekit.clients.friendly._RawSynchronousFlyteClient.update_project")
def test_update_project(mock_raw_update_project):
    client = _SynchronousFlyteClient(url="a.b.com", insecure=True)
    project = _Project("foo", "name", "description", state=_Project.ProjectState.ACTIVE)
    client.update_project(project)
    mock_raw_update_project.assert_called_with(project.to_flyte_idl())
