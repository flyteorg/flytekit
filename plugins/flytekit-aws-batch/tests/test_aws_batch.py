from flytekitplugins.awsbatch import AWSBatchConfig

from flytekit import PythonFunctionTask, task
from flytekit.extend import Image, ImageConfig, SerializationSettings

config = AWSBatchConfig(
    parameters={"codec": "mp4"},
    platformCapabilities=["EC2"],
    propagateTags=True,
    retryStrategy={"attempts": 10},
    tags={"hello": "world"},
    timeout={"attemptDurationSeconds": 60},
)


def test_aws_batch_task():
    @task(task_config=config)
    def t1(a: int) -> str:
        inc = a + 2
        return str(inc)

    assert t1.task_config is not None
    assert t1.task_config == config
    assert t1.task_type == "aws-batch"
    assert isinstance(t1, PythonFunctionTask)

    default_img = Image(name="default", fqn="test", tag="tag")
    settings = SerializationSettings(
        project="project",
        domain="domain",
        version="version",
        env={"FOO": "baz"},
        image_config=ImageConfig(default_image=default_img, images=[default_img]),
    )
    assert t1.get_custom(settings) == config.to_dict()
    assert t1.get_command(settings) == [
        "pyflyte-map-execute",
        "--inputs",
        "{{.input}}",
        "--output-prefix",
        "{{.outputPrefix}}",
        "--raw-output-data-prefix",
        "{{.rawOutputDataPrefix}}",
        "--is-aws-batch-single-job",
        "--resolver",
        "flytekit.core.python_auto_container.default_task_resolver",
        "--",
        "task-module",
        "tests.test_aws_batch",
        "task-name",
        "t1",
    ]
