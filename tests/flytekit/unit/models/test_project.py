from __future__ import absolute_import

from flytekit.models import project


def test_project():
    obj = project.Project("project_id", "project_name")
    assert obj.id == "project_id"
    assert obj.name == "project_name"
    assert obj == project.Project.from_flyte_idl(obj.to_flyte_idl())
