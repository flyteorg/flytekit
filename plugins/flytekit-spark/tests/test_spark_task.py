import pandas as pd
import pyspark
import pytest
from flytekitplugins.spark import Spark
from flytekitplugins.spark.task import new_spark_session
from pyspark.sql import SparkSession

import flytekit
from flytekit import StructuredDataset, StructuredDatasetTransformerEngine, task
from flytekit.configuration import Image, ImageConfig, SerializationSettings
from flytekit.core.context_manager import ExecutionParameters, FlyteContextManager


@pytest.fixture(scope="function")
def reset_spark_session() -> None:
    pyspark.sql.SparkSession.builder.getOrCreate().stop()
    yield
    pyspark.sql.SparkSession.builder.getOrCreate().stop()


def test_spark_task(reset_spark_session):
    @task(task_config=Spark(spark_conf={"spark": "1"}))
    def my_spark(a: str) -> int:
        session = flytekit.current_context().spark_session
        assert session.sparkContext.appName == "FlyteSpark: ex:local:local:local"
        return 10

    assert my_spark.task_config is not None
    assert my_spark.task_config.spark_conf == {"spark": "1"}

    default_img = Image(name="default", fqn="test", tag="tag")
    settings = SerializationSettings(
        project="project",
        domain="domain",
        version="version",
        env={"FOO": "baz"},
        image_config=ImageConfig(default_image=default_img, images=[default_img]),
    )

    retrieved_settings = my_spark.get_custom(settings)
    assert retrieved_settings["sparkConf"] == {"spark": "1"}

    pb = ExecutionParameters.new_builder()
    pb.working_dir = "/tmp"
    pb.execution_id = "ex:local:local:local"
    p = pb.build()
    new_p = my_spark.pre_execute(p)
    assert new_p is not None
    assert new_p.has_attr("SPARK_SESSION")

    assert my_spark.sess is not None
    configs = my_spark.sess.sparkContext.getConf().getAll()
    assert ("spark", "1") in configs
    assert ("spark.app.name", "FlyteSpark: ex:local:local:local") in configs


def test_new_spark_session():
    name = "SessionName"
    spark_conf = {"spark1": "1", "spark2": "2"}
    new_sess = new_spark_session(name, spark_conf)
    configs = new_sess.sparkContext.getConf().getAll()
    assert new_sess is not None
    assert ("spark.driver.bindAddress", "127.0.0.1") in configs
    assert ("spark.master", "local[*]") in configs
    assert ("spark1", "1") in configs
    assert ("spark2", "2") in configs


def test_to_html():
    spark = SparkSession.builder.getOrCreate()
    df = spark.createDataFrame([("Bob", 10)], ["name", "age"])
    sd = StructuredDataset(dataframe=df)
    tf = StructuredDatasetTransformerEngine()
    output = tf.to_html(FlyteContextManager.current_context(), sd, pyspark.sql.DataFrame)
    assert pd.DataFrame(df.schema, columns=["StructField"]).to_html() == output
