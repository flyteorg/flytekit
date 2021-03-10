from datetime import timedelta
from itertools import product

import pytest
from flyteidl.core.tasks_pb2 import TaskMetadata
from google.protobuf import text_format
from k8s.io.api.core.v1 import generated_pb2

from flytekit.models import literals, task
from flytekit.models.core import identifier
from tests.flytekit.common import parameterizers


def test_resource_entry():
    obj = task.Resources.ResourceEntry(task.Resources.ResourceName.CPU, "blah")
    assert task.Resources.ResourceEntry.from_flyte_idl(obj.to_flyte_idl()) == obj
    assert obj != task.Resources.ResourceEntry(task.Resources.ResourceName.GPU, "blah")
    assert obj != task.Resources.ResourceEntry(task.Resources.ResourceName.CPU, "bloop")
    assert obj.name == task.Resources.ResourceName.CPU
    assert obj.value == "blah"


@pytest.mark.parametrize("resource_list", parameterizers.LIST_OF_RESOURCE_ENTRY_LISTS)
def test_resources(resource_list):
    obj = task.Resources(resource_list, resource_list)
    obj1 = task.Resources([], resource_list)
    obj2 = task.Resources(resource_list, [])

    assert obj.requests == obj2.requests
    assert obj.limits == obj1.limits
    assert obj == task.Resources.from_flyte_idl(obj.to_flyte_idl())


def test_runtime_metadata():
    obj = task.RuntimeMetadata(task.RuntimeMetadata.RuntimeType.FLYTE_SDK, "1.0.0", "python")
    assert obj.type == task.RuntimeMetadata.RuntimeType.FLYTE_SDK
    assert obj.version == "1.0.0"
    assert obj.flavor == "python"
    assert obj == task.RuntimeMetadata.from_flyte_idl(obj.to_flyte_idl())
    assert obj != task.RuntimeMetadata(task.RuntimeMetadata.RuntimeType.FLYTE_SDK, "1.0.1", "python")
    assert obj != task.RuntimeMetadata(task.RuntimeMetadata.RuntimeType.OTHER, "1.0.0", "python")
    assert obj != task.RuntimeMetadata(task.RuntimeMetadata.RuntimeType.FLYTE_SDK, "1.0.0", "golang")


def test_task_metadata_interruptible_from_flyte_idl():
    # Interruptible not set
    idl = TaskMetadata()
    obj = task.TaskMetadata.from_flyte_idl(idl)
    assert obj.interruptible is None

    idl = TaskMetadata()
    idl.interruptible = True
    obj = task.TaskMetadata.from_flyte_idl(idl)
    assert obj.interruptible is True

    idl = TaskMetadata()
    idl.interruptible = False
    obj = task.TaskMetadata.from_flyte_idl(idl)
    assert obj.interruptible is False


def test_task_metadata():
    obj = task.TaskMetadata(
        True,
        task.RuntimeMetadata(task.RuntimeMetadata.RuntimeType.FLYTE_SDK, "1.0.0", "python"),
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
    assert obj.runtime.type == task.RuntimeMetadata.RuntimeType.FLYTE_SDK
    assert obj.runtime.version == "1.0.0"
    assert obj.deprecated_error_message == "This is deprecated!"
    assert obj.discovery_version == "0.1.1b0"
    assert obj == task.TaskMetadata.from_flyte_idl(obj.to_flyte_idl())


@pytest.mark.parametrize(
    "in_tuple",
    product(parameterizers.LIST_OF_TASK_METADATA, parameterizers.LIST_OF_INTERFACES, parameterizers.LIST_OF_RESOURCES),
)
def test_task_template(in_tuple):
    task_metadata, interfaces, resources = in_tuple
    obj = task.TaskTemplate(
        identifier.Identifier(identifier.ResourceType.TASK, "project", "domain", "name", "version"),
        "python",
        task_metadata,
        interfaces,
        {"a": 1, "b": {"c": 2, "d": 3}},
        container=task.Container(
            "my_image", ["this", "is", "a", "cmd"], ["this", "is", "an", "arg"], resources, {"a": "b"}, {"d": "e"},
        ),
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
        task.TaskTemplate.from_flyte_idl(obj.to_flyte_idl()).to_flyte_idl()
    )


@pytest.mark.parametrize("sec_ctx", parameterizers.LIST_OF_SECURITY_CONTEXT)
def test_task_template_security_context(sec_ctx):
    obj = task.TaskTemplate(
        identifier.Identifier(identifier.ResourceType.TASK, "project", "domain", "name", "version"),
        "python",
        parameterizers.LIST_OF_TASK_METADATA[0],
        parameterizers.LIST_OF_INTERFACES[0],
        {"a": 1, "b": {"c": 2, "d": 3}},
        container=task.Container(
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
        task.TaskTemplate.from_flyte_idl(obj.to_flyte_idl()).to_flyte_idl()
    )


@pytest.mark.parametrize("task_closure", parameterizers.LIST_OF_TASK_CLOSURES)
def test_task(task_closure):
    obj = task.Task(
        identifier.Identifier(identifier.ResourceType.TASK, "project", "domain", "name", "version"), task_closure,
    )
    assert obj.id.project == "project"
    assert obj.id.domain == "domain"
    assert obj.id.name == "name"
    assert obj.id.version == "version"
    assert obj.closure == task_closure
    assert obj == task.Task.from_flyte_idl(obj.to_flyte_idl())


@pytest.mark.parametrize("resources", parameterizers.LIST_OF_RESOURCES)
def test_container(resources):
    obj = task.Container(
        "my_image", ["this", "is", "a", "cmd"], ["this", "is", "an", "arg"], resources, {"a": "b"}, {"d": "e"},
    )
    obj.image == "my_image"
    obj.command == ["this", "is", "a", "cmd"]
    obj.args == ["this", "is", "an", "arg"]
    obj.resources == resources
    obj.env == {"a": "b"}
    obj.config == {"d": "e"}
    assert obj == task.Container.from_flyte_idl(obj.to_flyte_idl())


def test_sidecar_task():
    pod_spec = generated_pb2.PodSpec()
    container = generated_pb2.Container(name="containery")
    pod_spec.containers.extend([container])
    obj = task.SidecarJob(pod_spec=pod_spec, primary_container_name="primary")
    assert obj.primary_container_name == "primary"
    assert len(obj.pod_spec.containers) == 1
    assert obj.pod_spec.containers[0].name == "containery"

    obj2 = task.SidecarJob.from_flyte_idl(obj.to_flyte_idl())
    assert obj2 == obj


def test_dataloadingconfig():
    dlc = task.DataLoadingConfig(
        "s3://input/path", "s3://output/path", True, task.DataLoadingConfig.LITERALMAP_FORMAT_YAML,
    )
    dlc2 = task.DataLoadingConfig.from_flyte_idl(dlc.to_flyte_idl())
    assert dlc2 == dlc

    dlc = task.DataLoadingConfig(
        "s3://input/path",
        "s3://output/path",
        True,
        task.DataLoadingConfig.LITERALMAP_FORMAT_YAML,
        io_strategy=task.IOStrategy(),
    )
    dlc2 = task.DataLoadingConfig.from_flyte_idl(dlc.to_flyte_idl())
    assert dlc2 == dlc


def test_ioconfig():
    io = task.IOStrategy(task.IOStrategy.DOWNLOAD_MODE_NO_DOWNLOAD, task.IOStrategy.UPLOAD_MODE_NO_UPLOAD)
    assert io == task.IOStrategy.from_flyte_idl(io.to_flyte_idl())
