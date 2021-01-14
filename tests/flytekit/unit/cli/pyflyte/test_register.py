from mock import MagicMock

from flytekit.common.launch_plan import SdkLaunchPlan
from flytekit.common.tasks.task import SdkTask
from flytekit.common.workflow import SdkWorkflow


def test_register_workflows(mock_clirunner, monkeypatch):

    mock_register_task = MagicMock(return_value=MagicMock())
    monkeypatch.setattr(SdkTask, "register", mock_register_task)
    mock_register_workflow = MagicMock(return_value=MagicMock())
    monkeypatch.setattr(SdkWorkflow, "register", mock_register_workflow)
    mock_register_launch_plan = MagicMock(return_value=MagicMock())
    monkeypatch.setattr(SdkLaunchPlan, "register", mock_register_launch_plan)

    result = mock_clirunner("register", "-p", "project", "-d", "development", "-v", "--version", "workflows")

    assert result.exit_code == 0

    assert len(mock_register_task.mock_calls) == 4
    assert len(mock_register_workflow.mock_calls) == 1
    assert len(mock_register_launch_plan.mock_calls) == 1


def test_register_workflows_with_test_switch(mock_clirunner, monkeypatch):
    mock_register_task = MagicMock(return_value=MagicMock())
    monkeypatch.setattr(SdkTask, "register", mock_register_task)
    mock_register_workflow = MagicMock(return_value=MagicMock())
    monkeypatch.setattr(SdkWorkflow, "register", mock_register_workflow)
    mock_register_launch_plan = MagicMock(return_value=MagicMock())
    monkeypatch.setattr(SdkLaunchPlan, "register", mock_register_launch_plan)

    result = mock_clirunner("register", "-p", "project", "-d", "development", "-v", "--version", "--test", "workflows")

    assert result.exit_code == 0

    assert len(mock_register_task.mock_calls) == 0
    assert len(mock_register_workflow.mock_calls) == 0
    assert len(mock_register_launch_plan.mock_calls) == 0
