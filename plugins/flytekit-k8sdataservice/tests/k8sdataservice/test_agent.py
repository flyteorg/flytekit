import json
from dataclasses import asdict
from datetime import timedelta
from unittest.mock import patch, MagicMock, mock_open
import grpc
from google.protobuf import json_format
from flyteidl.admin.agent_pb2 import (
    SUCCEEDED,
    RUNNING,
    PENDING,
    PERMANENT_FAILURE
)
from flytekitplugins.k8sdataservice.task import DataServiceConfig
from flytekitplugins.k8sdataservice.agent import DataServiceMetadata
from google.protobuf.struct_pb2 import Struct
import flytekit.models.interface as interface_models
from flytekit.extend.backend.base_agent import AgentRegistry
from flytekit.interfaces.cli_identifiers import Identifier
from flytekit.models import literals, task, types
from flytekit.models.core.identifier import ResourceType
from flytekit.models.task import TaskTemplate


@patch("flytekitplugins.k8sdataservice.agent.open", new_callable=mock_open,
       read_data="task_logs:\n  templates:\n    - templateUris:\n        - 'https://some-log-url'\n     displayName: testlogs")
@patch("flytekitplugins.k8sdataservice.agent.yaml.safe_load")
@patch("flytekitplugins.k8sdataservice.agent.K8sManager.create_data_service", return_value="dummy_statefulset_name")
@patch("flytekitplugins.k8sdataservice.agent.K8sManager.check_stateful_set_status", return_value="succeeded")
@patch("flytekitplugins.k8sdataservice.agent.K8sManager.delete_stateful_set")
@patch("flytekitplugins.k8sdataservice.agent.K8sManager.delete_service")
def test_gnn_agent(mock_delete_service, mock_delete_stateful_set, mock_check_status, mock_create_data_service, mock_load, mock_open):
    ctx = MagicMock(spec=grpc.ServicerContext)
    mock_load.return_value = {"task_logs": {"templates": [{"templateUris": ["https://some-log-url"], "displayName": "testlogs"}]}}
    # Your test code here
    agent = AgentRegistry.get_agent("dataservicetask")
    task_id = Identifier(
        resource_type=ResourceType.TASK, project="project", domain="domain", name="name", version="version"
    )
    task_metadata = task.TaskMetadata(
        True,
        task.RuntimeMetadata(task.RuntimeMetadata.RuntimeType.FLYTE_SDK, "1.0.0", "python"),
        timedelta(days=1),
        literals.RetryStrategy(3),
        True,
        "0.1.1b0",
        "This is deprecated!",
        True,
        "A",
    )
    s = Struct()
    s.update({
        "Name": "gnn-1234",
        "Image": "image",
        "Command": "command",
        "Cluster": "ei-dev2"
    })
    task_config = json_format.MessageToDict(s)

    int_type = types.LiteralType(types.SimpleType.INTEGER)
    interfaces = interface_models.TypedInterface(
        {
            "a": interface_models.Variable(int_type, "description1"),
            "b": interface_models.Variable(int_type, "description2"),
        },
        {},
    )
    task_inputs = literals.LiteralMap(
        {
            "a": literals.Literal(scalar=literals.Scalar(primitive=literals.Primitive(integer=1))),
            "b": literals.Literal(scalar=literals.Scalar(primitive=literals.Primitive(integer=1))),
        },
    )

    dummy_template = TaskTemplate(
        id=task_id,
        custom=task_config,
        metadata=task_metadata,
        interface=interfaces,
        type="dataservicetask",
    )

    metadata_bytes = json.dumps(
        asdict(DataServiceMetadata(
            dataservice_config=DataServiceConfig(Name="gnn-1234", Image="image", Command="command", Cluster="ei-dev2"),
            name="dummy_statefulset_name"))
    ).encode("utf-8")

    # Test create method
    create_response = agent.create(ctx, "/tmp", dummy_template, task_inputs)
    assert create_response.resource_meta == metadata_bytes
    mock_create_data_service.assert_called_once()

    # Test get method
    res = agent.get(ctx, metadata_bytes)
    assert res.resource.state == SUCCEEDED
    mock_check_status.assert_called_once_with("dummy_statefulset_name")

    # # Test delete method
    agent.delete(ctx, metadata_bytes)
    mock_delete_stateful_set.assert_called_once_with("dummy_statefulset_name")
    mock_delete_service.assert_called_once_with("dummy_statefulset_name")


@patch("flytekitplugins.k8sdataservice.agent.open", new_callable=mock_open,
       read_data="task_logs:\n  templates:\n    - templateUris:\n        - 'https://some-log-url'\n     displayName: testlogs")
@patch("flytekitplugins.k8sdataservice.agent.yaml.safe_load")
@patch("flytekitplugins.k8sdataservice.agent.K8sManager.create_data_service", return_value="dummy_statefulset_name")
@patch("flytekitplugins.k8sdataservice.agent.K8sManager.check_stateful_set_status", return_value="succeeded")
@patch("flytekitplugins.k8sdataservice.agent.K8sManager.get_execution_id_from_existing", return_value="2345")
@patch("flytekitplugins.k8sdataservice.agent.K8sManager.delete_stateful_set")
@patch("flytekitplugins.k8sdataservice.agent.K8sManager.delete_service")
def test_gnn_agent_reuse_data_service(mock_delete_service, mock_delete_stateful_set,
                                      mock_get_execution_id_from_existing, mock_check_status, mock_create_data_service, mock_load, mock_open):
    ctx = MagicMock(spec=grpc.ServicerContext)
    mock_load.return_value = {"task_logs": {"templates": [{"templateUris": ["https://some-log-url"], "displayName": "testlogs"}]}}
    # Your test code here
    agent = AgentRegistry.get_agent("dataservicetask")
    task_id = Identifier(
        resource_type=ResourceType.TASK, project="project", domain="domain", name="name", version="version"
    )
    task_metadata = task.TaskMetadata(
        True,
        task.RuntimeMetadata(task.RuntimeMetadata.RuntimeType.FLYTE_SDK, "1.0.0", "python"),
        timedelta(days=1),
        literals.RetryStrategy(3),
        True,
        "0.1.1b0",
        "This is deprecated!",
        True,
        "A",
    )
    s = Struct()
    s.update({
        "Name": "gnn-2345",
        "Image": "image",
        "Command": "command",
        "Cluster": "ei-dev2",
        "ExistingReleaseName": "gnn-2345"
    })
    task_config = json_format.MessageToDict(s)

    int_type = types.LiteralType(types.SimpleType.INTEGER)
    interfaces = interface_models.TypedInterface(
        {
            "a": interface_models.Variable(int_type, "description1"),
            "b": interface_models.Variable(int_type, "description2"),
        },
        {},
    )
    task_inputs = literals.LiteralMap(
        {
            "a": literals.Literal(scalar=literals.Scalar(primitive=literals.Primitive(integer=1))),
            "b": literals.Literal(scalar=literals.Scalar(primitive=literals.Primitive(integer=1))),
        },
    )

    dummy_template = TaskTemplate(
        id=task_id,
        custom=task_config,
        metadata=task_metadata,
        interface=interfaces,
        type="dataservicetask",
    )

    metadata_bytes = json.dumps(
        asdict(DataServiceMetadata(
            dataservice_config=DataServiceConfig(
                Name="gnn-2345", Image="image", Command="command", Cluster="ei-dev2", ExistingReleaseName="gnn-2345"),
            name="gnn-2345"))
    ).encode("utf-8")

    # Test create method, and create_data_service should have not been called
    create_response = agent.create(ctx, "/tmp", dummy_template, task_inputs)
    assert create_response.resource_meta == metadata_bytes
    mock_create_data_service.assert_not_called()

    # Test get method
    res = agent.get(ctx, metadata_bytes)
    assert res.resource.state == SUCCEEDED
    mock_check_status.assert_called_once_with("gnn-2345")

    # # Test delete method
    agent.delete(ctx, metadata_bytes)
    mock_delete_stateful_set.assert_called_once_with("gnn-2345")
    mock_delete_service.assert_called_once_with("gnn-2345")


@patch("flytekitplugins.k8sdataservice.agent.open", new_callable=mock_open,
       read_data="task_logs:\n  templates:\n    - templateUris:\n        - 'https://some-log-url'\n     displayName: testlogs")
@patch("flytekitplugins.k8sdataservice.agent.yaml.safe_load")
@patch("flytekitplugins.k8sdataservice.agent.K8sManager.create_data_service", return_value="dummy_statefulset_name")
@patch("flytekitplugins.k8sdataservice.agent.K8sManager.check_stateful_set_status", return_value="running")
@patch("flytekitplugins.k8sdataservice.agent.K8sManager.get_execution_id_from_existing", return_value="2345")
@patch("flytekitplugins.k8sdataservice.agent.K8sManager.delete_stateful_set")
@patch("flytekitplugins.k8sdataservice.agent.K8sManager.delete_service")
def test_gnn_agent_status(mock_delete_service, mock_delete_stateful_set,
                          mock_get_execution_id_from_existing, mock_check_status, mock_create_data_service, mock_load, mock_open):
    ctx = MagicMock(spec=grpc.ServicerContext)
    mock_load.return_value = {"task_logs": {"templates": [{"templateUris": ["https://some-log-url"], "displayName": "testlogs"}]}}
    # Your test code here
    agent = AgentRegistry.get_agent("dataservicetask")
    task_id = Identifier(
        resource_type=ResourceType.TASK, project="project", domain="domain", name="name", version="version"
    )
    task_metadata = task.TaskMetadata(
        True,
        task.RuntimeMetadata(task.RuntimeMetadata.RuntimeType.FLYTE_SDK, "1.0.0", "python"),
        timedelta(days=1),
        literals.RetryStrategy(3),
        True,
        "0.1.1b0",
        "This is deprecated!",
        True,
        "A",
    )
    s = Struct()
    s.update({
        "Name": "gnn-2345",
        "Image": "image",
        "Command": "command",
        "Cluster": "ei-dev2",
        "ExistingReleaseName": "gnn-2345"
    })
    task_config = json_format.MessageToDict(s)

    int_type = types.LiteralType(types.SimpleType.INTEGER)
    interfaces = interface_models.TypedInterface(
        {
            "a": interface_models.Variable(int_type, "description1"),
            "b": interface_models.Variable(int_type, "description2"),
        },
        {},
    )
    task_inputs = literals.LiteralMap(
        {
            "a": literals.Literal(scalar=literals.Scalar(primitive=literals.Primitive(integer=1))),
            "b": literals.Literal(scalar=literals.Scalar(primitive=literals.Primitive(integer=1))),
        },
    )

    dummy_template = TaskTemplate(
        id=task_id,
        custom=task_config,
        metadata=task_metadata,
        interface=interfaces,
        type="dataservicetask",
    )

    metadata_bytes = json.dumps(
        asdict(DataServiceMetadata(
            dataservice_config=DataServiceConfig(
                Name="gnn-2345", Image="image", Command="command", Cluster="ei-dev2", ExistingReleaseName="gnn-2345"),
            name="gnn-2345"))
    ).encode("utf-8")

    # Test create method, and create_data_service should have not been called
    create_response = agent.create(ctx, "/tmp", dummy_template, task_inputs)
    assert create_response.resource_meta == metadata_bytes
    mock_create_data_service.assert_not_called()

    # Test get method
    res = agent.get(ctx, metadata_bytes)
    assert res.resource.state == RUNNING
    mock_check_status.assert_called_once_with("gnn-2345")

    # # Test delete methods are not called
    agent.delete(ctx, metadata_bytes)
    mock_delete_stateful_set.assert_called_once_with("gnn-2345")
    mock_delete_service.assert_called_once_with("gnn-2345")


@patch("flytekitplugins.k8sdataservice.agent.open", new_callable=mock_open,
       read_data="")
@patch("flytekitplugins.k8sdataservice.agent.yaml.safe_load")
@patch("flytekitplugins.k8sdataservice.agent.K8sManager.create_data_service", return_value="dummy_statefulset_name")
@patch("flytekitplugins.k8sdataservice.agent.K8sManager.check_stateful_set_status", return_value="succeeded")
@patch("flytekitplugins.k8sdataservice.agent.K8sManager.get_execution_id_from_existing", return_value="2345")
@patch("flytekitplugins.k8sdataservice.agent.K8sManager.delete_stateful_set")
@patch("flytekitplugins.k8sdataservice.agent.K8sManager.delete_service")
def test_gnn_agent_no_configmap(mock_delete_service, mock_delete_stateful_set,
                                mock_get_execution_id_from_existing, mock_check_status, mock_create_data_service, mock_load, mock_open):
    ctx = MagicMock(spec=grpc.ServicerContext)
    mock_load.return_value = {}
    # Your test code here
    agent = AgentRegistry.get_agent("dataservicetask")
    task_id = Identifier(
        resource_type=ResourceType.TASK, project="project", domain="domain", name="name", version="version"
    )
    task_metadata = task.TaskMetadata(
        True,
        task.RuntimeMetadata(task.RuntimeMetadata.RuntimeType.FLYTE_SDK, "1.0.0", "python"),
        timedelta(days=1),
        literals.RetryStrategy(3),
        True,
        "0.1.1b0",
        "This is deprecated!",
        True,
        "A",
    )
    s = Struct()
    s.update({
        "Name": "gnn-2345",
        "Image": "image",
        "Command": "command",
        "Cluster": "ei-dev2",
        "ExistingReleaseName": "gnn-2345"
    })
    task_config = json_format.MessageToDict(s)

    int_type = types.LiteralType(types.SimpleType.INTEGER)
    interfaces = interface_models.TypedInterface(
        {
            "a": interface_models.Variable(int_type, "description1"),
            "b": interface_models.Variable(int_type, "description2"),
        },
        {},
    )
    task_inputs = literals.LiteralMap(
        {
            "a": literals.Literal(scalar=literals.Scalar(primitive=literals.Primitive(integer=1))),
            "b": literals.Literal(scalar=literals.Scalar(primitive=literals.Primitive(integer=1))),
        },
    )

    dummy_template = TaskTemplate(
        id=task_id,
        custom=task_config,
        metadata=task_metadata,
        interface=interfaces,
        type="dataservicetask",
    )

    metadata_bytes = json.dumps(
        asdict(DataServiceMetadata(
            dataservice_config=DataServiceConfig(
                Name="gnn-2345", Image="image", Command="command", Cluster="ei-dev2", ExistingReleaseName="gnn-2345"),
            name="gnn-2345"))
    ).encode("utf-8")

    # Test create method, and create_data_service should have not been called
    create_response = agent.create(ctx, "/tmp", dummy_template, task_inputs)
    assert create_response.resource_meta == metadata_bytes
    mock_create_data_service.assert_not_called()

    # Test get method
    res = agent.get(ctx, metadata_bytes)
    assert res.resource.state == SUCCEEDED
    mock_check_status.assert_called_once_with("gnn-2345")

    # # Test delete methods are not called
    agent.delete(ctx, metadata_bytes)
    mock_delete_stateful_set.assert_called_once_with("gnn-2345")
    mock_delete_service.assert_called_once_with("gnn-2345")


@patch("flytekitplugins.k8sdataservice.agent.open", new_callable=mock_open,
       read_data="")
@patch("flytekitplugins.k8sdataservice.agent.yaml.safe_load")
@patch("flytekitplugins.k8sdataservice.agent.K8sManager.create_data_service", return_value="dummy_statefulset_name")
@patch("flytekitplugins.k8sdataservice.agent.K8sManager.check_stateful_set_status", return_value="pending")
@patch("flytekitplugins.k8sdataservice.agent.K8sManager.get_execution_id_from_existing", return_value="2345")
@patch("flytekitplugins.k8sdataservice.agent.K8sManager.delete_stateful_set")
@patch("flytekitplugins.k8sdataservice.agent.K8sManager.delete_service")
def test_gnn_agent_status_failed(mock_delete_service, mock_delete_stateful_set,
                                 mock_get_execution_id_from_existing, mock_check_status, mock_create_data_service, mock_load, mock_open):
    ctx = MagicMock(spec=grpc.ServicerContext)
    mock_load.return_value = {}
    # Your test code here
    agent = AgentRegistry.get_agent("dataservicetask")
    task_id = Identifier(
        resource_type=ResourceType.TASK, project="project", domain="domain", name="name", version="version"
    )
    task_metadata = task.TaskMetadata(
        True,
        task.RuntimeMetadata(task.RuntimeMetadata.RuntimeType.FLYTE_SDK, "1.0.0", "python"),
        timedelta(days=1),
        literals.RetryStrategy(3),
        True,
        "0.1.1b0",
        "This is deprecated!",
        True,
        "A",
    )
    s = Struct()
    s.update({
        "Name": "gnn-2345",
        "Image": "image",
        "Command": "command",
        "Cluster": "ei-dev2",
        "ExistingReleaseName": "gnn-2345"
    })
    task_config = json_format.MessageToDict(s)

    int_type = types.LiteralType(types.SimpleType.INTEGER)
    interfaces = interface_models.TypedInterface(
        {
            "a": interface_models.Variable(int_type, "description1"),
            "b": interface_models.Variable(int_type, "description2"),
        },
        {},
    )
    task_inputs = literals.LiteralMap(
        {
            "a": literals.Literal(scalar=literals.Scalar(primitive=literals.Primitive(integer=1))),
            "b": literals.Literal(scalar=literals.Scalar(primitive=literals.Primitive(integer=1))),
        },
    )

    dummy_template = TaskTemplate(
        id=task_id,
        custom=task_config,
        metadata=task_metadata,
        interface=interfaces,
        type="dataservicetask",
    )

    metadata_bytes = json.dumps(
        asdict(DataServiceMetadata(
            dataservice_config=DataServiceConfig(
                Name="gnn-2345", Image="image", Command="command", Cluster="ei-dev2", ExistingReleaseName="gnn-2345"),
            name="gnn-2345"))
    ).encode("utf-8")

    # Test create method, and create_data_service should have not been called
    create_response = agent.create(ctx, "/tmp", dummy_template, task_inputs)
    assert create_response.resource_meta == metadata_bytes
    mock_create_data_service.assert_not_called()

    # Test get method
    res = agent.get(ctx, metadata_bytes)
    assert res.resource.state == PENDING
    mock_check_status.assert_called_once_with("gnn-2345")

    mock_check_status.return_value = "failed"
    res.resource.state == PERMANENT_FAILURE

    # # Test delete methods are not called
    agent.delete(ctx, metadata_bytes)
    mock_delete_stateful_set.assert_called_once_with("gnn-2345")
    mock_delete_service.assert_called_once_with("gnn-2345")
