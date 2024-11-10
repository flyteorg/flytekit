from unittest.mock import patch, MagicMock, ANY
from datetime import datetime
from kingkong.execution import (
    HelmExecutionContext,
    Execution,
    ResourceRequirements,
    ContainerStateWaiting,
    ContainerView,
    PodView,
    ExecutionView,
    create_kingkong_execution_id,
    create_king_kong_execution_label
)


def test_helm_execution_context_init():
    context = HelmExecutionContext(type="typeA", config_name="config1", installation_name="install1")
    assert context.type == "typeA"
    assert context.config_name == "config1"
    assert context.installation_name == "install1"


def test_execution_init():
    execution = Execution(created_by="user1", exec_type="Custom", namespace="namespace1")
    assert execution.created_by == "user1"
    assert execution.type == "Custom"
    assert execution.namespace == "namespace1"
    assert execution.helm_context is None
    assert execution.custom_context is None


def test_resource_requirements_init():
    resources = ResourceRequirements(limits={"cpu": "1000m"}, requests={"cpu": "500m"})
    assert resources.limits["cpu"] == "1000m"
    assert resources.requests["cpu"] == "500m"


def test_container_state_waiting_init():
    state = ContainerStateWaiting(reason="Pending", message="Waiting for resources")
    assert state.reason == "Pending"
    assert state.message == "Waiting for resources"


def test_container_view_init():
    resources = ResourceRequirements()
    container_view = ContainerView(image="image1", name="container1", restart_count=1, resources=resources, state={"running": "true"})
    assert container_view.image == "image1"
    assert container_view.name == "container1"
    assert container_view.restart_count == 1
    assert container_view.resources == resources
    assert container_view.state["running"] == "true"


def test_pod_view_initialization():
    resources = ResourceRequirements(limits={"cpu": "1000m"}, requests={"cpu": "500m"})
    container1 = ContainerView(image="image1", name="container1", restart_count=1, resources=resources, state={"running": "true"})
    container2 = ContainerView(image="image2", name="container2", restart_count=2, resources=resources, state={"waiting": "false"})
    containers = [container1, container2]
    last_transition_time = datetime(2024, 10, 28, 15, 0, 0)
    finished_at = datetime(2024, 10, 28, 16, 0, 0)
    started_at = datetime(2024, 10, 28, 14, 0, 0)
    k8s_resource_id = "pod-12345"
    node_name = "node-1"
    phase = "Running"
    pod_view = PodView(
        last_transition_time=last_transition_time,
        finished_at=finished_at,
        started_at=started_at,
        k8s_resource_id=k8s_resource_id,
        node_name=node_name,
        phase=phase,
        containers=containers
    )
    assert pod_view.last_transition_time == last_transition_time
    assert pod_view.finished_at == finished_at
    assert pod_view.started_at == started_at
    assert pod_view.k8s_resource_id == k8s_resource_id
    assert pod_view.node_name == node_name
    assert pod_view.phase == phase
    assert pod_view.containers == containers
    assert len(pod_view.containers) == 2
    assert pod_view.containers[0].name == "container1"
    assert pod_view.containers[1].name == "container2"


def test_execution_view_init():
    execution = Execution(created_by="user", exec_type="type1", namespace="namespace")
    exec_view = ExecutionView(
        execution=execution, execution_id=123, started_at=None, ended_at=None, status="running", resources=None
    )
    assert exec_view.execution == execution
    assert exec_view.id == 123
    assert exec_view.status == "running"


@patch("kingkong.execution.get_kingkong_endpoint", return_value="https://example.com/api/v1/execution")
@patch("kingkong.execution.get_execution_namespace", return_value="test-namespace")
@patch("requests.post")
@patch("kingkong.execution.logger")
def test_create_kingkong_execution_id_success(mock_logger, mock_post, mock_namespace, mock_endpoint):
    mock_response = MagicMock()
    mock_response.status_code = 201
    mock_response.json.return_value = {
        "id": 12345,
        "startedAt": "2024-10-28T00:00:00",
        "endedAt": "2024-10-28T01:00:00",
        "status": "Completed"
    }
    mock_post.return_value = mock_response
    result = create_kingkong_execution_id(cluster="test-cluster")

    assert result == 12345
    mock_logger.info.assert_called_once_with("Using kingkongConfig.Namespace: test-namespace")
    mock_post.assert_called_once_with(
        "https://example.com/api/v1/execution",
        data=ANY,
        headers={"Content-Type": "application/json"},
        verify=False
    )


@patch("kingkong.execution.get_kingkong_endpoint", return_value="https://example.com/api/v1/execution")
@patch("kingkong.execution.get_execution_namespace", return_value="test-namespace")
@patch("requests.post")
@patch("kingkong.execution.logger")
def test_create_kingkong_execution_id_failure(mock_logger, mock_post, mock_namespace, mock_endpoint):
    mock_response = MagicMock()
    mock_response.status_code = 500
    mock_post.return_value = mock_response
    result = create_kingkong_execution_id(cluster="test-cluster")
    assert result is None
    mock_logger.error.assert_called_once_with("Error occurred in posting to King Kong: Request failed with status 500")


@patch("kingkong.execution.logger")
def test_create_king_kong_execution_label(mock_logger):
    label = create_king_kong_execution_label("12345")
    assert label == {"kong.linkedin.com/executionID": "12345"}
    mock_logger.info.assert_called_once_with("the kingkong execution id is: 12345")
