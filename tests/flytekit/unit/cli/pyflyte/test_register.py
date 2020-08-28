from mock import MagicMock

from flytekit.engines.flyte import engine


def test_register_workflows(mock_clirunner, monkeypatch):
    mock_get_task = MagicMock()
    monkeypatch.setattr(engine.FlyteEngineFactory, "get_task", MagicMock(return_value=mock_get_task))
    mock_get_workflow = MagicMock()
    monkeypatch.setattr(
        engine.FlyteEngineFactory, "get_workflow", MagicMock(return_value=mock_get_workflow),
    )
    mock_get_launch_plan = MagicMock()
    monkeypatch.setattr(
        engine.FlyteEngineFactory, "get_launch_plan", MagicMock(return_value=mock_get_launch_plan),
    )

    result = mock_clirunner("register", "workflows")

    assert result.exit_code == 0

    assert len(mock_get_task.mock_calls) == 4
    assert len(mock_get_task.register.mock_calls) == 4
    assert len(mock_get_workflow.mock_calls) == 1
    assert len(mock_get_workflow.register.mock_calls) == 1
    assert len(mock_get_launch_plan.mock_calls) == 1
    assert len(mock_get_launch_plan.register.mock_calls) == 1


def test_register_workflows_with_test_switch(mock_clirunner, monkeypatch):
    mock_get_task = MagicMock()
    monkeypatch.setattr(engine.FlyteEngineFactory, "get_task", MagicMock(return_value=mock_get_task))
    mock_get_workflow = MagicMock()
    monkeypatch.setattr(
        engine.FlyteEngineFactory, "get_workflow", MagicMock(return_value=mock_get_workflow),
    )
    mock_get_launch_plan = MagicMock()
    monkeypatch.setattr(
        engine.FlyteEngineFactory, "get_launch_plan", MagicMock(return_value=mock_get_launch_plan),
    )

    result = mock_clirunner("register", "--test", "workflows")

    assert result.exit_code == 0

    assert len(mock_get_task.mock_calls) == 0
    assert len(mock_get_workflow.mock_calls) == 0
    assert len(mock_get_launch_plan.mock_calls) == 0
