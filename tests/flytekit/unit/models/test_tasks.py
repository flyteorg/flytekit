from datetime import timedelta
from itertools import product

import pytest
from flyteidl.core.tasks_pb2 import TaskMetadata
from google.protobuf import text_format
from k8s.io.api.core.v1 import generated_pb2

import flytekit.models.core.interface as interface_models
import flytekit.models.core.literals as literal_models
import flytekit.models.core.task
import flytekit.models.core.types
from flytekit.models import task
from flytekit.models.core import identifier, literals
from flytekit.models.core.task import TaskMetadata as _taskMatadata, TaskTemplate as _taskTemplate
from flytekit.models.admin.core.task import RuntimeMetadata as _runtimeMetadata
from flytekit.models.admin.task import Task as _task
from tests.flytekit.common import parameterizers


def test_resource_entry():
    obj = flytekit.models.core.task.Resources.ResourceEntry(flytekit.models.core.task.Resources.ResourceName.CPU, "blah")
    assert flytekit.models.core.task.Resources.ResourceEntry.from_flyte_idl(obj.to_flyte_idl()) == obj
    assert obj != flytekit.models.core.task.Resources.ResourceEntry(
        flytekit.models.core.task.Resources.ResourceName.GPU, "blah")
    assert obj != flytekit.models.core.task.Resources.ResourceEntry(
        flytekit.models.core.task.Resources.ResourceName.CPU, "bloop")
    assert obj.name == flytekit.models.core.task.Resources.ResourceName.CPU
    assert obj.value == "blah"


@pytest.mark.parametrize("resource_list", parameterizers.LIST_OF_RESOURCE_ENTRY_LISTS)
def test_resources(resource_list):
    obj = flytekit.models.core.task.Resources(resource_list, resource_list)
    obj1 = flytekit.models.core.task.Resources([], resource_list)
    obj2 = flytekit.models.core.task.Resources(resource_list, [])

    assert obj.requests == obj2.requests
    assert obj.limits == obj1.limits
    assert obj == flytekit.models.core.task.Resources.from_flyte_idl(obj.to_flyte_idl())


def test_runtime_metadata():
    obj = _runtimeMetadata(_runtimeMetadata.RuntimeType.FLYTE_SDK, "1.0.0", "python")
    assert obj.type == _runtimeMetadata.RuntimeType.FLYTE_SDK
    assert obj.version == "1.0.0"
    assert obj.flavor == "python"
    assert obj == _runtimeMetadata.from_flyte_idl(obj.to_flyte_idl())
    assert obj != _runtimeMetadata(_runtimeMetadata.RuntimeType.FLYTE_SDK, "1.0.1", "python")
    assert obj != _runtimeMetadata(_runtimeMetadata.RuntimeType.OTHER, "1.0.0", "python")
    assert obj != _runtimeMetadata(_runtimeMetadata.RuntimeType.FLYTE_SDK, "1.0.0", "golang")


def test_task_metadata_interruptible_from_flyte_idl():
    # Interruptible not set
    idl = TaskMetadata()
    obj = _taskMatadata.from_flyte_idl(idl)
    assert obj.interruptible is None

    idl = TaskMetadata()
    idl.interruptible = True
    obj = _taskMatadata.from_flyte_idl(idl)
    assert obj.interruptible is True

    idl = TaskMetadata()
    idl.interruptible = False
    obj = _taskMatadata.from_flyte_idl(idl)
    assert obj.interruptible is False


def test_task_metadata():
    obj = _taskMatadata(
        True,
        _runtimeMetadata(_runtimeMetadata.RuntimeType.FLYTE_SDK, "1.0.0", "python"),
        timedelta(days=1),
        literals.RetryStrategy(3),
        True,
        "0.1.1b0",
        "This is deprecated!",
    )

    assert obj.discoverable is True
    assert obj.retries.retries == 3
    assert obj.interruptible is True
    assert obj.timeout == timedelta(days=1)
    assert obj.runtime.flavor == "python"
    assert obj.runtime.type == _runtimeMetadata.RuntimeType.FLYTE_SDK
    assert obj.runtime.version == "1.0.0"
    assert obj.deprecated_error_message == "This is deprecated!"
    assert obj.discovery_version == "0.1.1b0"
    assert obj == _taskMatadata.from_flyte_idl(obj.to_flyte_idl())


@pytest.mark.parametrize(
    "in_tuple",
    product(parameterizers.LIST_OF_TASK_METADATA, parameterizers.LIST_OF_INTERFACES, parameterizers.LIST_OF_RESOURCES),
)
def test_task_template(in_tuple):
    task_metadata, interfaces, resources = in_tuple
    obj = _taskTemplate(
        identifier.Identifier(identifier.ResourceType.TASK, "project", "domain", "name", "version"),
        "python",
        task_metadata,
        interfaces,
        {"a": 1, "b": {"c": 2, "d": 3}},
        container=flytekit.models.core.task.Container(
            "my_image",
            ["this", "is", "a", "cmd"],
            ["this", "is", "an", "arg"],
            resources,
            {"a": "b"},
            {"d": "e"},
        ),
        config={"a": "b"},
    )
    assert obj.id.resource_type == identifier.ResourceType.TASK
    assert obj.id.project == "project"
    assert obj.id.domain == "domain"
    assert obj.id.name == "name"
    assert obj.id.version == "version"
    assert obj.type == "python"
    assert obj.metadata == task_metadata
    assert obj.interface == interfaces
    assert obj.custom == {"a": 1, "b": {"c": 2, "d": 3}}
    assert obj.container.image == "my_image"
    assert obj.container.resources == resources
    assert text_format.MessageToString(obj.to_flyte_idl()) == text_format.MessageToString(
        _taskTemplate.from_flyte_idl(obj.to_flyte_idl()).to_flyte_idl()
    )
    assert obj.config == {"a": "b"}


def test_task_template__k8s_pod_target():
    int_type = flytekit.models.core.types.LiteralType(flytekit.models.core.types.SimpleType.INTEGER)
    obj = _taskTemplate(
        identifier.Identifier(identifier.ResourceType.TASK, "project", "domain", "name", "version"),
        "python",
        _taskMatadata(
            False,
            _runtimeMetadata(1, "v", "f"),
            timedelta(days=1),
            literal_models.RetryStrategy(5),
            False,
            "1.0",
            "deprecated",
        ),
        interface_models.TypedInterface(
            # inputs
            {"a": interface_models.Variable(int_type, "description1")},
            # outputs
            {
                "b": interface_models.Variable(int_type, "description2"),
                "c": interface_models.Variable(int_type, "description3"),
            },
        ),
        {"a": 1, "b": {"c": 2, "d": 3}},
        config={"a": "b"},
        k8s_pod=flytekit.models.core.task.K8sPod(
            metadata=flytekit.models.core.task.K8sObjectMetadata(labels={"label": "foo"}, annotations={"anno": "bar"}),
            pod_spec={"str": "val", "int": 1},
        ),
    )
    assert obj.id.resource_type == identifier.ResourceType.TASK
    assert obj.id.project == "project"
    assert obj.id.domain == "domain"
    assert obj.id.name == "name"
    assert obj.id.version == "version"
    assert obj.type == "python"
    assert obj.custom == {"a": 1, "b": {"c": 2, "d": 3}}
    assert obj.k8s_pod.metadata == flytekit.models.core.task.K8sObjectMetadata(labels={"label": "foo"}, annotations={"anno": "bar"})
    assert obj.k8s_pod.pod_spec == {"str": "val", "int": 1}
    assert text_format.MessageToString(obj.to_flyte_idl()) == text_format.MessageToString(
        _taskTemplate.from_flyte_idl(obj.to_flyte_idl()).to_flyte_idl()
    )
    assert obj.config == {"a": "b"}


@pytest.mark.parametrize("sec_ctx", parameterizers.LIST_OF_SECURITY_CONTEXT)
def test_task_template_security_context(sec_ctx):
    obj = _taskTemplate(
        identifier.Identifier(identifier.ResourceType.TASK, "project", "domain", "name", "version"),
        "python",
        parameterizers.LIST_OF_TASK_METADATA[0],
        parameterizers.LIST_OF_INTERFACES[0],
        {"a": 1, "b": {"c": 2, "d": 3}},
        container=flytekit.models.core.task.Container(
            "my_image",
            ["this", "is", "a", "cmd"],
            ["this", "is", "an", "arg"],
            parameterizers.LIST_OF_RESOURCES[0],
            {"a": "b"},
            {"d": "e"},
        ),
        security_context=sec_ctx,
    )
    assert obj.security_context == sec_ctx
    assert text_format.MessageToString(obj.to_flyte_idl()) == text_format.MessageToString(
        _taskTemplate.from_flyte_idl(obj.to_flyte_idl()).to_flyte_idl()
    )


@pytest.mark.parametrize("task_closure", parameterizers.LIST_OF_TASK_CLOSURES)
def test_task(task_closure):
    obj = _task(
        identifier.Identifier(identifier.ResourceType.TASK, "project", "domain", "name", "version"),
        task_closure,
    )
    assert obj.id.project == "project"
    assert obj.id.domain == "domain"
    assert obj.id.name == "name"
    assert obj.id.version == "version"
    assert obj.closure == task_closure
    assert obj == _task.from_flyte_idl(obj.to_flyte_idl())


@pytest.mark.parametrize("resources", parameterizers.LIST_OF_RESOURCES)
def test_container(resources):
    obj = flytekit.models.core.task.Container(
        "my_image",
        ["this", "is", "a", "cmd"],
        ["this", "is", "an", "arg"],
        resources,
        {"a": "b"},
        {"d": "e"},
    )
    obj.image == "my_image"
    obj.command == ["this", "is", "a", "cmd"]
    obj.args == ["this", "is", "an", "arg"]
    obj.resources == resources
    obj.env == {"a": "b"}
    obj.config == {"d": "e"}
    assert obj == flytekit.models.core.task.Container.from_flyte_idl(obj.to_flyte_idl())


def test_sidecar_task():
    pod_spec = generated_pb2.PodSpec()
    container = generated_pb2.Container(name="containery")
    pod_spec.containers.extend([container])
    obj = flytekit.models.core.task.SidecarJob(
        pod_spec=pod_spec,
        primary_container_name="primary",
        annotations={"a1": "a1"},
        labels={"b1": "b1"},
    )
    assert obj.primary_container_name == "primary"
    assert len(obj.pod_spec.containers) == 1
    assert obj.pod_spec.containers[0].name == "containery"
    assert obj.annotations["a1"] == "a1"
    assert obj.labels["b1"] == "b1"

    obj2 = flytekit.models.core.task.SidecarJob.from_flyte_idl(obj.to_flyte_idl())
    assert obj2 == obj


def test_sidecar_task_label_annotation_not_provided():
    pod_spec = generated_pb2.PodSpec()
    obj = flytekit.models.core.task.SidecarJob(pod_spec=pod_spec, primary_container_name="primary")

    assert obj.primary_container_name == "primary"

    obj2 = flytekit.models.core.task.SidecarJob.from_flyte_idl(obj.to_flyte_idl())
    assert obj2 == obj


def test_dataloadingconfig():
    dlc = flytekit.models.core.task.DataLoadingConfig(
        "s3://input/path",
        "s3://output/path",
        True,
        flytekit.models.core.task.DataLoadingConfig.LITERALMAP_FORMAT_YAML,
    )
    dlc2 = flytekit.models.core.task.DataLoadingConfig.from_flyte_idl(dlc.to_flyte_idl())
    assert dlc2 == dlc

    dlc = flytekit.models.core.task.DataLoadingConfig(
        "s3://input/path",
        "s3://output/path",
        True,
        flytekit.models.core.task.DataLoadingConfig.LITERALMAP_FORMAT_YAML,
        io_strategy=flytekit.models.core.task.IOStrategy(),
    )
    dlc2 = flytekit.models.core.task.DataLoadingConfig.from_flyte_idl(dlc.to_flyte_idl())
    assert dlc2 == dlc


def test_ioconfig():
    io = flytekit.models.core.task.IOStrategy(flytekit.models.core.task.IOStrategy.DOWNLOAD_MODE_NO_DOWNLOAD, flytekit.models.core.task.IOStrategy.UPLOAD_MODE_NO_UPLOAD)
    assert io == flytekit.models.core.task.IOStrategy.from_flyte_idl(io.to_flyte_idl())


def test_k8s_metadata():
    obj = flytekit.models.core.task.K8sObjectMetadata(labels={"label": "foo"}, annotations={"anno": "bar"})
    assert obj.labels == {"label": "foo"}
    assert obj.annotations == {"anno": "bar"}
    assert obj == flytekit.models.core.task.K8sObjectMetadata.from_flyte_idl(obj.to_flyte_idl())


def test_k8s_pod():
    obj = flytekit.models.core.task.K8sPod(metadata=flytekit.models.core.task.K8sObjectMetadata(labels={"label": "foo"}), pod_spec={"pod_spec": "bar"})
    assert obj.metadata.labels == {"label": "foo"}
    assert obj.pod_spec == {"pod_spec": "bar"}
    assert obj == flytekit.models.core.task.K8sPod.from_flyte_idl(obj.to_flyte_idl())
