import mock
from k8s.io.api.core.v1 import generated_pb2

from flytekit.common.tasks import sdk_dynamic as _sdk_dynamic
from flytekit.common.tasks import sdk_runnable as _sdk_runnable
from flytekit.common.tasks import sidecar_task as _sidecar_task
from flytekit.configuration.internal import IMAGE as _IMAGE
from flytekit.sdk.tasks import dynamic_sidecar_task, inputs, outputs, python_task
from flytekit.sdk.types import Types


def get_pod_spec():
    a_container = generated_pb2.Container(name="main")
    a_container.command.extend(["foo", "bar"])
    a_container.volumeMounts.extend([generated_pb2.VolumeMount(name="scratch", mountPath="/scratch",)])

    pod_spec = generated_pb2.PodSpec(restartPolicy="Never",)
    pod_spec.containers.extend([a_container, generated_pb2.Container(name="sidecar")])
    return pod_spec


with mock.patch.object(_IMAGE, "get", return_value="docker.io/blah:abc123"):

    @outputs(out1=Types.String)
    @python_task
    def simple_python_task(wf_params, out1):
        out1.set("test")

    @inputs(in1=Types.Integer)
    @outputs(out1=Types.String)
    @dynamic_sidecar_task(
        cpu_request="10",
        memory_limit="2Gi",
        environment={"foo": "bar"},
        pod_spec=get_pod_spec(),
        primary_container_name="main",
    )
    def simple_dynamic_sidecar_task(wf_params, in1, out1):
        yield simple_python_task()


def test_dynamic_sidecar_task():
    assert isinstance(simple_dynamic_sidecar_task, _sdk_runnable.SdkRunnableTask)
    assert isinstance(simple_dynamic_sidecar_task, _sidecar_task.SdkDynamicSidecarTask)
    assert isinstance(simple_dynamic_sidecar_task, _sidecar_task.SdkSidecarTask)
    assert isinstance(simple_dynamic_sidecar_task, _sdk_dynamic.SdkDynamicTaskMixin)

    pod_spec = simple_dynamic_sidecar_task.custom["podSpec"]
    assert pod_spec["restartPolicy"] == "Never"
    assert len(pod_spec["containers"]) == 2
    primary_container = pod_spec["containers"][0]
    assert primary_container["name"] == "main"
    assert primary_container["args"] == [
        "pyflyte-execute",
        "--task-module",
        "tests.flytekit.unit.sdk.tasks.test_dynamic_sidecar_tasks",
        "--task-name",
        "simple_dynamic_sidecar_task",
        "--inputs",
        "{{.input}}",
        "--output-prefix",
        "{{.outputPrefix}}",
        "--raw-output-data-prefix",
        "{{.rawOutputDataPrefix}}",
    ]
    assert primary_container["volumeMounts"] == [{"mountPath": "/scratch", "name": "scratch"}]
    assert {"name": "foo", "value": "bar"} in primary_container["env"]
    assert primary_container["resources"] == {
        "requests": {"cpu": {"string": "10"}},
        "limits": {"memory": {"string": "2Gi"}},
    }
    assert pod_spec["containers"][1]["name"] == "sidecar"
    assert simple_dynamic_sidecar_task.custom["primaryContainerName"] == "main"
