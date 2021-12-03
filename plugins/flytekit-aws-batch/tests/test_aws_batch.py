from flytekitplugins.awsbatch import AWSBatch
from flytekitplugins.spark.task import new_spark_session

import flytekit
from flytekit import task
from flytekit.common.tasks.sdk_runnable import ExecutionParameters
from flytekit.core.map_task import MapPythonTask
from flytekit.extend import Image, ImageConfig, SerializationSettings


def test_spark_task():
    @task(task_config=AWSBatch(concurrency=3))
    def mapper(a: int) -> str:
        inc = a + 2
        return str(inc)

    assert mapper.aws_batch_config is not None
    assert mapper.aws_batch_config == AWSBatch(concurrency=3)
    # assert isinstance(mapper, MapPythonTask)

    default_img = Image(name="default", fqn="test", tag="tag")
    settings = SerializationSettings(
        project="project",
        domain="domain",
        version="version",
        env={"FOO": "baz"},
        image_config=ImageConfig(default_image=default_img, images=[default_img]),
    )

    assert mapper.get_custom(settings) == {"parallelism": "3"}
