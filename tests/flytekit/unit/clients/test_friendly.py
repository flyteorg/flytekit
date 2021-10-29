import mock as _mock
from flyteidl.admin import project_pb2 as _project_pb2

from flytekit.clients.friendly import SynchronousFlyteClient as _SynchronousFlyteClient
from flytekit.models.admin.project import Project as _Project


@_mock.patch("flytekit.clients.friendly._RawSynchronousFlyteClient.update_project")
def test_update_project(mock_raw_update_project):
    client = _SynchronousFlyteClient(url="a.b.com", insecure=True)
    project = _Project("foo", "name", "description", state=_Project.ProjectState.ACTIVE)
    client.update_project(project)
    mock_raw_update_project.assert_called_with(project.to_flyte_idl())


@_mock.patch("flytekit.clients.friendly._RawSynchronousFlyteClient.list_projects")
def test_list_projects_paginated(mock_raw_list_projects):
    client = _SynchronousFlyteClient(url="a.b.com", insecure=True)
    client.list_projects_paginated(limit=100, token="")
    project_list_request = _project_pb2.ProjectListRequest(limit=100, token="", filters=None, sort_by=None)
    mock_raw_list_projects.assert_called_with(project_list_request=project_list_request)
