import mock
from k8s.io.api.core.v1 import generated_pb2

import flytekit.platform.sdk_task
from flytekit.common.tasks import sidecar_task as _sidecar_task
from flytekit.configuration.internal import IMAGE as _IMAGE
from flytekit.models.core import identifier as _identifier
from flytekit.sdk.tasks import inputs, outputs, sidecar_task
from flytekit.sdk.types import Types


def get_pod_spec():
    a_container = generated_pb2.Container(name="a container",)
    a_container.command.extend(["fee", "fi", "fo", "fum"])
    a_container.volumeMounts.extend([generated_pb2.VolumeMount(name="volume mount", mountPath="some/where",)])

    pod_spec = generated_pb2.PodSpec(restartPolicy="OnFailure",)
    pod_spec.containers.extend([a_container, generated_pb2.Container(name="another container")])
    return pod_spec


with mock.patch.object(_IMAGE, "get", return_value="docker.io/blah:abc123"):

    @inputs(in1=Types.Integer)
    @outputs(out1=Types.String)
    @sidecar_task(
        cpu_request="10",
        gpu_limit="2",
        environment={"foo": "bar"},
        pod_spec=get_pod_spec(),
        primary_container_name="a container",
    )
    def simple_sidecar_task(wf_params, in1, out1):
        pass


simple_sidecar_task._id = _identifier.Identifier(_identifier.ResourceType.TASK, "project", "domain", "name", "version")


def test_sidecar_task():
    assert isinstance(simple_sidecar_task, flytekit.platform.sdk_task.SdkTask)
    assert isinstance(simple_sidecar_task, _sidecar_task.SdkSidecarTask)

    pod_spec = simple_sidecar_task.custom["podSpec"]
    assert pod_spec["restartPolicy"] == "OnFailure"
    assert len(pod_spec["containers"]) == 2
    primary_container = pod_spec["containers"][0]
    assert primary_container["name"] == "a container"
    assert primary_container["args"] == [
        "pyflyte-execute",
        "--task-module",
        "tests.flytekit.unit.sdk.tasks.test_sidecar_tasks",
        "--task-name",
        "simple_sidecar_task",
        "--inputs",
        "{{.input}}",
        "--output-prefix",
        "{{.outputPrefix}}",
        "--raw-output-data-prefix",
        "{{.rawOutputDataPrefix}}",
    ]
    assert primary_container["volumeMounts"] == [{"mountPath": "some/where", "name": "volume mount"}]
    assert {"name": "foo", "value": "bar"} in primary_container["env"]
    assert primary_container["resources"] == {
        "requests": {"cpu": {"string": "10"}},
        "limits": {"gpu": {"string": "2"}},
    }
    assert pod_spec["containers"][1]["name"] == "another container"
    assert simple_sidecar_task.custom["primaryContainerName"] == "a container"
