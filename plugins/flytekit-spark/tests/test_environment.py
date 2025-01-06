import pyspark
import pytest

import flytekit
from flytekitplugins.spark import Spark
from flytekit.core.environment import Environment


@pytest.fixture(scope="function")
def reset_spark_session() -> None: # type: ignore

    pyspark.sql.SparkSession.builder.getOrCreate().stop()
    yield
    pyspark.sql.SparkSession.builder.getOrCreate().stop()


def test_spark_task(reset_spark_session):

    env = Environment(
        task_config=Spark(
            spark_conf={"spark": "1"},
            executor_path="/usr/bin/python3",
            applications_path="local:///usr/local/bin/entrypoint.py",
        )
    )

    @env.task
    def my_spark(a: str) -> int:
        session = flytekit.current_context().spark_session
        assert session.sparkContext.appName == "FlyteSpark: ex:local:local:local"
        return 10

    assert my_spark.task_config is not None
    assert my_spark.task_config.spark_conf == {"spark": "1"}
