from flytekitplugins.ray import RayClientConfig, RayConfig, RayJobSubmissionConfig

from flytekit import PythonFunctionTask, task
from flytekit.configuration import Image, ImageConfig, SerializationSettings

config = RayConfig(
    ray_client_config=RayClientConfig(address="127.0.0.1"),
    ray_job_submission_config=RayJobSubmissionConfig(entrypoint="python script.py"),
)


def test_ray_task():
    @task(task_config=config)
    def t1(a: int) -> str:
        inc = a + 2
        return str(inc)

    assert t1.task_config is not None
    assert t1.task_config == config
    assert t1.task_type == "ray"
    assert isinstance(t1, PythonFunctionTask)

    default_img = Image(name="default", fqn="test", tag="tag")
    settings = SerializationSettings(
        project="proj",
        domain="dom",
        version="123",
        image_config=ImageConfig(default_image=default_img, images=[default_img]),
        env={},
    )

    assert t1.get_custom(settings) == config.to_dict()
    assert t1.get_command(settings) == [
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
        "tests.test_ray",
        "task-name",
        "t1",
    ]
