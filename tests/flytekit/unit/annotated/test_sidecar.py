from k8s.io.api.core.v1 import generated_pb2

from flytekit.annotated.context_manager import Image, ImageConfig, RegistrationSettings
from flytekit.annotated.resources import Resources
from flytekit.annotated.task import task
from flytekit.taskplugins.sidecar.task import Sidecar, SidecarFunctionTask


def get_pod_spec():
    a_container = generated_pb2.Container(name="a container",)
    a_container.command.extend(["fee", "fi", "fo", "fum"])
    a_container.volumeMounts.extend([generated_pb2.VolumeMount(name="volume mount", mountPath="some/where",)])

    pod_spec = generated_pb2.PodSpec(restartPolicy="OnFailure",)
    pod_spec.containers.extend([a_container, generated_pb2.Container(name="another container")])
    return pod_spec


def test_sidecar_task():
    sidecar = Sidecar(pod_spec=get_pod_spec(), primary_container_name="a container")

    @task(task_config=sidecar, requests=Resources(cpu="10"), limits=Resources(gpu="2"), environment={"FOO": "bar"})
    def simple_sidecar_task(i: int):
        pass

    assert isinstance(simple_sidecar_task, SidecarFunctionTask)
    assert simple_sidecar_task.task_config == sidecar

    default_img = Image(name="default", fqn="test", tag="tag")

    custom = simple_sidecar_task.get_custom(
        RegistrationSettings(
            project="project",
            domain="domain",
            version="version",
            env={"FOO": "baz"},
            image_config=ImageConfig(default_image=default_img, images=[default_img]),
        )
    )
    assert custom["podSpec"]["restartPolicy"] == "OnFailure"
    assert len(custom["podSpec"]["containers"]) == 2
    primary_container = custom["podSpec"]["containers"][0]
    assert primary_container["name"] == "a container"
    assert primary_container["args"] == [
        "pyflyte-execute",
        "--task-module",
        "test_sidecar",
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
    assert primary_container["resources"] == {
        "requests": {"cpu": {"string": "10"}},
        "limits": {"gpu": {"string": "2"}},
    }
    assert primary_container["env"] == [{"name": "FOO", "value": "bar"}]
    assert custom["podSpec"]["containers"][1]["name"] == "another container"
    assert custom["primaryContainerName"] == "a container"
