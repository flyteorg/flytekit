import pyspark
import pytest

import flytekit
from flytekitplugins.spark import Spark
from flytekit.core.environment import Environment, inherit


def test_basic_environment():

    env = Environment(retries=2)

    @env.task
    def foo():
        pass

    @env
    def bar():
        pass

    assert foo._metadata.retries == 2
    assert bar._metadata.retries == 2

def test_extended_environment():

    env = Environment(retries=2)

    other = env.extend(retries=0)

    @other.task
    def foo():
        pass

    @other
    def bar():
        pass


    assert foo._metadata.retries == 0
    assert bar._metadata.retries == 0

def test_updated_environment():

    env = Environment(retries=2)

    env.update(retries=0)

    @env.task
    def foo():
        pass

    @env
    def bar():
        pass


    assert foo._metadata.retries == 0
    assert bar._metadata.retries == 0

def test_show_environment():

    env = Environment(retries=2)

    env.show()

def test_inherit():

    old_config = {"cache": False, "timeout": 10}

    new_config = {"cache": True}

    combined = inherit(old_config, new_config)

    assert combined["cache"] == True
    assert combined["timeout"] == 10


@pytest.fixture(scope="function")
def reset_spark_session() -> None:

    pyspark.sql.SparkSession.builder.getOrCreate().stop()
    yield
    pyspark.sql.SparkSession.builder.getOrCreate().stop()


def test_spark_task(reset_spark_session):
    databricks_conf = {
        "name": "flytekit databricks plugin example",
        "new_cluster": {
            "spark_version": "11.0.x-scala2.12",
            "node_type_id": "r3.xlarge",
            "aws_attributes": {"availability": "ON_DEMAND"},
            "num_workers": 4,
            "docker_image": {"url": "pingsutw/databricks:latest"},
        },
        "timeout_seconds": 3600,
        "max_retries": 1,
        "spark_python_task": {
            "python_file": "dbfs:///FileStore/tables/entrypoint-1.py",
            "parameters": "ls",
        },
    }

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
