from mock import patch as _patch

from flytekit.common.tasks.task import SdkTask
from flytekit.models import interface as _interface
from flytekit.models import task as _task_model
from flytekit.models import types as _types
from flytekit.models.core import identifier as _identifier
from flytekit.models.task import CompiledTask, Task, TaskClosure
from tests.flytekit.unit.common_tests.test_workflow_promote import get_sample_container, get_sample_task_metadata


@_patch("flytekit.engines.flyte.engine.get_client")
def test_task_fetch_sets_correct_things(mock_get_client):
    int_type = _types.LiteralType(_types.SimpleType.INTEGER)
    task_interface = _interface.TypedInterface(
        # inputs
        {"a": _interface.Variable(int_type, "description1")},
        # outputs
        {"b": _interface.Variable(int_type, "description2"), "c": _interface.Variable(int_type, "description3")},
    )
    # Since the promotion of a workflow requires retrieving the task from Admin, we mock the SdkTask to return
    task_id = _identifier.Identifier(
        _identifier.ResourceType.TASK, "project", "domain", "test.task.for.fetch", "version",
    )
    task_template = _task_model.TaskTemplate(
        task_id,
        "python_container",
        get_sample_task_metadata(),
        task_interface,
        custom={},
        container=get_sample_container(),
    )

    task_from_client = Task(id=task_id, closure=TaskClosure(compiled_task=CompiledTask(template=task_template)))
    mock_get_client.return_value.get_task.return_value = task_from_client
    fetched = SdkTask.fetch("project", "domain", "test.task.for.fetch", "version")
    assert fetched._has_registered
    assert fetched.platform_valid_name == "test.task.for.fetch"
