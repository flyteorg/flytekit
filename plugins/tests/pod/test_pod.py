from typing import List

from flytekitplugins.pod.task import Pod, PodFunctionTask
from k8s.io.api.core.v1 import generated_pb2

from flytekit import Resources, dynamic, task
from flytekit.core import context_manager
from flytekit.extend import ExecutionState, Image, ImageConfig, SerializationSettings


def get_pod_spec():
    a_container = generated_pb2.Container(name="a container",)
    a_container.command.extend(["fee", "fi", "fo", "fum"])
    a_container.volumeMounts.extend([generated_pb2.VolumeMount(name="volume mount", mountPath="some/where",)])

    pod_spec = generated_pb2.PodSpec(restartPolicy="OnFailure",)
    pod_spec.containers.extend([a_container, generated_pb2.Container(name="another container")])
    return pod_spec


def test_pod_task():
    pod = Pod(pod_spec=get_pod_spec(), primary_container_name="a container")

    @task(task_config=pod, requests=Resources(cpu="10"), limits=Resources(gpu="2"), environment={"FOO": "bar"})
    def simple_pod_task(i: int):
        pass

    assert isinstance(simple_pod_task, PodFunctionTask)
    assert simple_pod_task.task_config == pod

    default_img = Image(name="default", fqn="test", tag="tag")

    custom = simple_pod_task.get_custom(
        SerializationSettings(
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
        "--inputs",
        "{{.input}}",
        "--output-prefix",
        "{{.outputPrefix}}",
        "--raw-output-data-prefix",
        "{{.rawOutputDataPrefix}}",
        "--resolver",
        "flytekit.core.python_auto_container.default_task_resolver",
        "--",
        "task-module",
        "plugins.tests.pod.test_pod",
        "task-name",
        "simple_pod_task",
    ]
    assert primary_container["volumeMounts"] == [{"mountPath": "some/where", "name": "volume mount"}]
    assert primary_container["resources"] == {
        "requests": {"cpu": {"string": "10"}},
        "limits": {"gpu": {"string": "2"}},
    }
    assert primary_container["env"] == [{"name": "FOO", "value": "bar"}]
    assert custom["podSpec"]["containers"][1]["name"] == "another container"
    assert custom["primaryContainerName"] == "a container"


def test_dynamic_pod_task():
    dynamic_pod = Pod(pod_spec=get_pod_spec(), primary_container_name="a container")

    @task
    def t1(a: int) -> int:
        return a + 10

    @dynamic(
        task_config=dynamic_pod, requests=Resources(cpu="10"), limits=Resources(gpu="2"), environment={"FOO": "bar"}
    )
    def dynamic_pod_task(a: int) -> List[int]:
        s = []
        for i in range(a):
            s.append(t1(a=i))
        return s

    assert isinstance(dynamic_pod_task, PodFunctionTask)
    default_img = Image(name="default", fqn="test", tag="tag")

    custom = dynamic_pod_task.get_custom(
        SerializationSettings(
            project="project",
            domain="domain",
            version="version",
            env={"FOO": "baz"},
            image_config=ImageConfig(default_image=default_img, images=[default_img]),
        )
    )
    assert len(custom["podSpec"]["containers"]) == 2
    primary_container = custom["podSpec"]["containers"][0]
    assert isinstance(dynamic_pod_task.task_config, Pod)
    assert primary_container["resources"] == {
        "requests": {"cpu": {"string": "10"}},
        "limits": {"gpu": {"string": "2"}},
    }

    with context_manager.FlyteContext.current_context().new_serialization_settings(
        serialization_settings=SerializationSettings(
            project="test_proj",
            domain="test_domain",
            version="abc",
            image_config=ImageConfig(Image(name="name", fqn="image", tag="name")),
            env={},
        )
    ) as ctx:
        with ctx.new_execution_context(mode=ExecutionState.Mode.TASK_EXECUTION) as ctx:
            dynamic_job_spec = dynamic_pod_task.compile_into_workflow(ctx, dynamic_pod_task._task_function, a=5)
            assert len(dynamic_job_spec._nodes) == 5
