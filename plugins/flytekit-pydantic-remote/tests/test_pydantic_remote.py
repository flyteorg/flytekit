import pytest
from flytekitplugins.pydantic_remote.pydantic_remote import PydanticFlyteRemote
from mock import patch

from flytekit.configuration import Config
from flytekit.models.admin.workflow import Workflow, WorkflowClosure
from flytekit.models.core.identifier import Identifier, ResourceType
from flytekit.models.task import Task
from tests.flytekit.common.parameterizers import LIST_OF_TASK_CLOSURES
from tests.flytekit.unit.remote.test_remote import get_compiled_workflow_closure


@pytest.fixture
def remote():
    with patch("flytekit.clients.friendly.SynchronousFlyteClient") as mock_client:
        flyte_remote = PydanticFlyteRemote(
            config=Config.auto(), default_project="p", default_domain="d"
        )
        flyte_remote._client_initialized = True
        flyte_remote._client = mock_client
        return flyte_remote


def test_fetch_pydantic_workflow(remote: PydanticFlyteRemote) -> None:
    mock_client = remote._client
    mock_client.get_task.return_value = Task(
        id=Identifier(ResourceType.TASK, "p", "d", "n", "v"),
        closure=LIST_OF_TASK_CLOSURES[0],
    )

    mock_client.get_workflow.return_value = Workflow(
        id=Identifier(ResourceType.TASK, "p", "d", "n", "v"),
        closure=WorkflowClosure(compiled_workflow=get_compiled_workflow_closure()),
    )
    wf = remote.fetch_pydantic_workflow(name="n", version="v")
    assert wf.__name__ == "PydanticWorkflowInterface (n)"
    assert (wf._project, wf._domain, wf._name, wf._version) == ("p", "d", "n", "v")
