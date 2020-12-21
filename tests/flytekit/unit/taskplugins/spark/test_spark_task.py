import flytekit
from flytekit import task
from flytekit.annotated.context_manager import Image, ImageConfig, RegistrationSettings
from flytekit.taskplugins.spark import Spark


def test_spark_task():
    @task(task_config=Spark(spark_conf={"spark": "1"}))
    def my_spark(a: str) -> int:
        session = flytekit.current_context().spark_session
        assert session.sparkContext.appName == "FlyteSpark: ex:local:local:local"
        return 10

    assert my_spark.task_config is not None
    assert my_spark.task_config.spark_conf == {"spark": "1"}

    default_img = Image(name="default", fqn="test", tag="tag")
    reg = RegistrationSettings(
        project="project",
        domain="domain",
        version="version",
        env={"FOO": "baz"},
        image_config=ImageConfig(default_image=default_img, images=[default_img]),
    )

    assert my_spark.get_custom(reg) == {
        "executorPath": "/Users/ketanumare/.virtualenvs/flytekit/bin/python",
        "mainApplicationFile": "local:///Users/ketanumare/src/flytekit/flytekit/bin/entrypoint.py",
        "sparkConf": {"spark": "1"},
    }
