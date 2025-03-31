from collections import OrderedDict
from unittest.mock import patch, MagicMock, mock_open
import pytest
from flytekitplugins.k8sdataservice.task import DataServiceConfig, DataServiceTask
from google.protobuf import json_format
from google.protobuf.struct_pb2 import Struct
from flytekit import kwtypes, Resources
from flytekit.configuration import ImageConfig, SerializationSettings
from flytekit.extend import get_serializable


@pytest.fixture
def mock_k8s_manager():
    with patch('flytekitplugins.k8sdataservice.connector.K8sManager') as MockK8sManager:
        mock_k8s_manager = MagicMock()
        MockK8sManager.return_value = mock_k8s_manager
        yield mock_k8s_manager


def test_gnn_task():
    gnn_config = DataServiceConfig(
        Name="test",
        Replicas=8,
        Image="image",
        Command=None,
        Cluster="grid2",
        Requests=Resources(cpu="1", mem="2", gpu="4"),
        Limits=Resources(cpu="1", mem="2", gpu="4"),
        Port=1234,
    )
    gnn_task = DataServiceTask(
        name="flytekit.poc.gnn.task",
        inputs=kwtypes(ds=str),
        task_config=gnn_config,
    )

    serialization_settings = SerializationSettings(
        project="test",
        domain="lnkd",
        image_config=ImageConfig.auto(),
        env={},
    )

    task_spec = get_serializable(OrderedDict(), serialization_settings, gnn_task)
    s = Struct()
    s.update({
        "Name": "test",
        "Replicas": 8,
        "Image": "image",
        "Command": None,
        "Cluster": "grid2",
        "ExistingReleaseName": None,
        "Requests": {
            "cpu": "1",
            "mem": "2",
            "gpu": "4",
            "ephemeral_storage": None
        },
        "Limits": {
            "cpu": "1",
            "mem": "2",
            "gpu": "4",
            "ephemeral_storage": None
        },
        "Port": 1234,
    })
    assert task_spec.template.custom == json_format.MessageToDict(s)


def test_gnn_task_optional_field():
    gnn_config = DataServiceConfig(
        Name="test",
        Replicas=8,
        Image="image",
        Command=None,
        Cluster="grid2",
        Port=1234,
    )
    gnn_task = DataServiceTask(
        name="flytekit.poc.gnn.task",
        inputs=kwtypes(ds=str),
        task_config=gnn_config,
    )

    serialization_settings = SerializationSettings(
        project="test",
        domain="lnkd",
        image_config=ImageConfig.auto(),
        env={},
    )

    task_spec = get_serializable(OrderedDict(), serialization_settings, gnn_task)
    s = Struct()
    s.update({
        "Name": "test",
        "Replicas": 8,
        "Image": "image",
        "Command": None,
        "Cluster": "grid2",
        "ExistingReleaseName": None,
        "Port": 1234,
    })
    assert task_spec.template.custom == json_format.MessageToDict(s)


def test_local_exec(mock_k8s_manager):
    gnn_config = DataServiceConfig(
        Replicas=8,
        Image="image",
        Command=[
            "bash",
            "-c",
            "command",
        ],
        Cluster="grid2",
    )
    gnn_task = DataServiceTask(
        name="flytekit.poc.gnn.task",
        inputs=kwtypes(ds=str),
        task_config=gnn_config,
    )
    assert gnn_task is not None
    assert len(gnn_task.interface.inputs) == 1
    assert len(gnn_task.interface.outputs) == 1

    # will not run locally
    with pytest.raises(Exception):
        gnn_task(ds="GNNTEST")
