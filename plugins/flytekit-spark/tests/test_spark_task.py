import os
import os.path
from unittest import mock

import pandas as pd
import pyspark
import pytest
import tempfile

from google.protobuf.json_format import MessageToDict
from flytekit import PodTemplate
from flytekit.core import context_manager
from flytekitplugins.spark import Spark
from flytekitplugins.spark.task import Databricks, DatabricksV2, new_spark_session
from pyspark.sql import SparkSession

import flytekit
from flytekit import (
    StructuredDataset,
    StructuredDatasetTransformerEngine,
    task,
    ImageSpec,
)
from flytekit.configuration import (
    Image,
    ImageConfig,
    SerializationSettings,
    FastSerializationSettings,
    DefaultImages,
)
from flytekit.core.context_manager import (
    ExecutionParameters,
    FlyteContextManager,
    ExecutionState,
)
from flytekit.models.task import K8sObjectMetadata, K8sPod
from kubernetes.client.models import (
    V1Container,
    V1PodSpec,
    V1Toleration,
    V1EnvVar,
)


@pytest.fixture(scope="function")
def reset_spark_session() -> None:
    if SparkSession._instantiatedSession:
        SparkSession.builder.getOrCreate().stop()
        SparkSession._instantiatedSession = None
    yield
    if SparkSession._instantiatedSession:
        SparkSession.builder.getOrCreate().stop()
        SparkSession._instantiatedSession = None

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

    @task(
        task_config=Spark(
            spark_conf={"spark": "1"},
            executor_path="/usr/bin/python3",
            applications_path="local:///usr/local/bin/entrypoint.py",
        )
    )
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
    assert retrieved_settings["executorPath"] == "/usr/bin/python3"
    assert (
        retrieved_settings["mainApplicationFile"]
        == "local:///usr/local/bin/entrypoint.py"
    )

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

    databricks_instance = "account.cloud.databricks.com"

    @task(
        task_config=Databricks(
            spark_conf={"spark": "2"},
            databricks_conf=databricks_conf,
            databricks_instance="account.cloud.databricks.com",
        )
    )
    def my_databricks(a: int) -> int:
        session = flytekit.current_context().spark_session
        assert session.sparkContext.appName == "FlyteSpark: ex:local:local:local"
        return a

    assert my_databricks.task_config is not None
    assert my_databricks.task_config.spark_conf == {"spark": "2"}
    assert my_databricks.task_config.databricks_conf == databricks_conf
    assert my_databricks.task_config.databricks_instance == databricks_instance
    assert my_databricks(a=3) == 3


@pytest.mark.parametrize("spark_conf", [None, {"spark": "2"}])
def test_databricks_v2(reset_spark_session, spark_conf):
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

    databricks_instance = "account.cloud.databricks.com"

    @task(
        task_config=DatabricksV2(
            databricks_conf=databricks_conf,
            databricks_instance=databricks_instance,
            spark_conf=spark_conf,
        )
    )
    def my_databricks(a: int) -> int:
        session = flytekit.current_context().spark_session
        assert session.sparkContext.appName == "FlyteSpark: ex:local:local:local"
        return a

    assert my_databricks.task_config is not None
    assert my_databricks.task_config.databricks_conf == databricks_conf
    assert my_databricks.task_config.databricks_instance == databricks_instance
    assert my_databricks.task_config.spark_conf == (spark_conf or {})
    assert my_databricks(a=3) == 3


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
    output = tf.to_html(
        FlyteContextManager.current_context(), sd, pyspark.sql.DataFrame
    )
    assert pd.DataFrame(df.schema, columns=["StructField"]).to_html() == output


@mock.patch("tempfile.mkdtemp", return_value="/tmp/123")
@mock.patch("shutil.make_archive")
@mock.patch("pyspark.context.SparkContext.addPyFile")
def test_spark_addPyFile(mock_add_pyfile, mock_shutil_make_archive, mock_tempfile_mkdtemp):
    @task(
        task_config=Spark(
            spark_conf={"spark": "1"},
        )
    )
    def my_spark(a: int) -> int:
        return a

    default_img = Image(name="default", fqn="test", tag="tag")
    serialization_settings = SerializationSettings(
        project="project",
        domain="domain",
        version="version",
        env={"FOO": "baz"},
        image_config=ImageConfig(default_image=default_img, images=[default_img]),
        fast_serialization_settings=FastSerializationSettings(
            enabled=True,
            destination_dir="/User/flyte/workflows",
            distribution_location="s3://my-s3-bucket/fast/123",
        ),
    )

    ctx = context_manager.FlyteContextManager.current_context()
    with context_manager.FlyteContextManager.with_context(
        ctx.with_execution_state(
            ctx.new_execution_state().with_params(
                mode=ExecutionState.Mode.TASK_EXECUTION
            )
        ).with_serialization_settings(serialization_settings)
    ) as new_ctx:
        my_spark.pre_execute(new_ctx.user_space_params)

        mock_tempfile_mkdtemp.assert_called_once()
        mock_shutil_make_archive.assert_called_once_with("/tmp/123/flyte_wf", "zip", os.getcwd())
        mock_add_pyfile.assert_called_once_with("/tmp/123/flyte_wf.zip")

def test_spark_with_image_spec():
    custom_image = ImageSpec(
        registry="ghcr.io/flyteorg",
        packages=["flytekitplugins-spark"],
    )

    @task(
        task_config=Spark(spark_conf={"spark.driver.memory": "1000M"}),
        container_image=custom_image,
    )
    def spark1(partitions: int) -> float:
        print("Starting Spark with Partitions: {}".format(partitions))
        return 1.0

    assert (
        spark1.container_image.base_image
        == f"cr.flyte.org/flyteorg/flytekit:spark-{DefaultImages.get_version_suffix()}"
    )
    assert spark1._default_executor_path == "/usr/bin/python3"
    assert spark1._default_applications_path == "local:///usr/local/bin/entrypoint.py"

    @task(
        task_config=Spark(spark_conf={"spark.driver.memory": "1000M"}),
        container_image=custom_image,
    )
    def spark2(partitions: int) -> float:
        print("Starting Spark with Partitions: {}".format(partitions))
        return 1.0

    assert (
        spark2.container_image.base_image
        == f"cr.flyte.org/flyteorg/flytekit:spark-{DefaultImages.get_version_suffix()}"
    )
    assert spark2._default_executor_path == "/usr/bin/python3"
    assert spark2._default_applications_path == "local:///usr/local/bin/entrypoint.py"


def clean_dict(d):
    """
    Recursively remove keys with None values from dictionaries and lists.
    """
    if isinstance(d, dict):
        return {k: clean_dict(v) for k, v in d.items() if v is not None}
    elif isinstance(d, list):
        return [clean_dict(item) for item in d if item is not None]
    else:
        return d


def test_spark_driver_executor_podSpec(reset_spark_session):
    custom_image = ImageSpec(
        registry="ghcr.io/flyteorg",
        packages=["flytekitplugins-spark"],
    )

    driver_pod_spec = V1PodSpec(
        containers=[
            V1Container(
                name="driver-primary",
                image="ghcr.io/flyteorg",
                command=["echo"],
                args=["wow"],
                env=[V1EnvVar(name="x/custom-driver", value="driver")],
            ),
            V1Container(
                name="not-primary",
                image="ghcr.io/flyteorg",
                command=["echo"],
                args=["not_primary"],
            ),
        ],
        tolerations=[
            V1Toleration(
                key="x/custom-driver",
                operator="Equal",
                value="foo-driver",
                effect="NoSchedule",
            ),
        ],
    )

    executor_pod_spec = V1PodSpec(
        containers=[
            V1Container(
                name="executor-primary",
                image="ghcr.io/flyteorg",
                command=["echo"],
                args=["wow"],
                env=[V1EnvVar(name="x/custom-executor", value="executor")],
            ),
            V1Container(
                name="not-primary",
                image="ghcr.io/flyteorg",
                command=["echo"],
                args=["not_primary"],
            ),
        ],
        tolerations=[
            V1Toleration(
                key="x/custom-executor",
                operator="Equal",
                value="foo-executor",
                effect="NoSchedule",
            ),
        ],
    )

    driver_pod = PodTemplate(
        labels={"lKeyA_d": "lValA", "lKeyB_d": "lValB"},
        annotations={"aKeyA_d": "aValA", "aKeyB_d": "aValB"},
        primary_container_name="driver-primary",
        pod_spec=driver_pod_spec,
    )

    executor_pod = PodTemplate(
        labels={"lKeyA_e": "lValA", "lKeyB_e": "lValB"},
        annotations={"aKeyA_e": "aValA", "aKeyB_e": "aValB"},
        primary_container_name="executor-primary",
        pod_spec=executor_pod_spec,
    )

    expect_driver_pod_spec = V1PodSpec(
        containers=[
            V1Container(
                name="driver-primary",
                image="ghcr.io/flyteorg",
                command=["echo"],
                args=["wow"],
                env=[
                    V1EnvVar(name="x/custom-driver", value="driver"),
                ],
            ),
            V1Container(
                name="not-primary",
                image="ghcr.io/flyteorg",
                command=["echo"],
                args=["not_primary"],
            ),
        ],
        tolerations=[
            V1Toleration(
                key="x/custom-driver",
                operator="Equal",
                value="foo-driver",
                effect="NoSchedule",
            ),
        ],
    )

    expect_executor_pod_spec = V1PodSpec(
        containers=[
            V1Container(
                name="executor-primary",
                image="ghcr.io/flyteorg",
                command=["echo"],
                args=["wow"],
                env=[
                    V1EnvVar(name="x/custom-executor", value="executor"),
                ],
            ),
            V1Container(
                name="not-primary",
                image="ghcr.io/flyteorg",
                command=["echo"],
                args=["not_primary"],
            ),
        ],
        tolerations=[
            V1Toleration(
                key="x/custom-executor",
                operator="Equal",
                value="foo-executor",
                effect="NoSchedule",
            ),
        ],
    )

    driver_pod_spec_dict_remove_None = expect_driver_pod_spec.to_dict()
    executor_pod_spec_dict_remove_None = expect_executor_pod_spec.to_dict()

    driver_pod_spec_dict_remove_None = clean_dict(driver_pod_spec_dict_remove_None)
    executor_pod_spec_dict_remove_None = clean_dict(executor_pod_spec_dict_remove_None)

    target_driver_k8sPod = K8sPod(
        metadata=K8sObjectMetadata(
            labels={"lKeyA_d": "lValA", "lKeyB_d": "lValB"},
            annotations={"aKeyA_d": "aValA", "aKeyB_d": "aValB"},
        ),
        pod_spec=driver_pod_spec_dict_remove_None,  # type: ignore
        primary_container_name="driver-primary"
    )

    target_executor_k8sPod = K8sPod(
        metadata=K8sObjectMetadata(
            labels={"lKeyA_e": "lValA", "lKeyB_e": "lValB"},
            annotations={"aKeyA_e": "aValA", "aKeyB_e": "aValB"},
        ),
        pod_spec=executor_pod_spec_dict_remove_None,  # type: ignore
        primary_container_name="executor-primary"
    )

    @task(
        task_config=Spark(
            spark_conf={"spark.driver.memory": "1000M"},
            driver_pod=driver_pod,
            executor_pod=executor_pod,
        ),
        container_image=custom_image,
        pod_template=PodTemplate(primary_container_name="primary"),
    )
    def my_spark(a: str) -> int:
        session = flytekit.current_context().spark_session
        configs = session.sparkContext.getConf().getAll()
        assert ("spark.driver.memory", "1000M") in configs
        assert session.sparkContext.appName == "FlyteSpark: ex:local:local:local"
        return 10

    assert my_spark.task_config is not None
    assert my_spark.task_config.spark_conf == {"spark.driver.memory": "1000M"}
    default_img = Image(name="default", fqn="test", tag="tag")

    settings = SerializationSettings(
        project="project",
        domain="domain",
        version="version",
        env={"FOO": "baz"},
        image_config=ImageConfig(default_image=default_img, images=[default_img]),
    )

    retrieved_settings = my_spark.get_custom(settings)
    assert retrieved_settings["sparkConf"] == {"spark.driver.memory": "1000M"}
    assert retrieved_settings["executorPath"] == "/usr/bin/python3"
    assert (
        retrieved_settings["mainApplicationFile"]
        == "local:///usr/local/bin/entrypoint.py"
    )
    assert retrieved_settings["driverPod"] == MessageToDict(
        target_driver_k8sPod.to_flyte_idl()
    )
    assert retrieved_settings["executorPod"] == MessageToDict(
        target_executor_k8sPod.to_flyte_idl()
    )

    pb = ExecutionParameters.new_builder()
    pb.working_dir = "/tmp"
    pb.execution_id = "ex:local:local:local"
    p = pb.build()
    new_p = my_spark.pre_execute(p)
    assert new_p is not None
    assert new_p.has_attr("SPARK_SESSION")

    assert my_spark.sess is not None
    configs = my_spark.sess.sparkContext.getConf().getAll()
    assert ("spark.driver.memory", "1000M") in configs
    assert ("spark.app.name", "FlyteSpark: ex:local:local:local") in configs


# ==================== Serverless Detection Tests ====================


def test_databricks_v2_serverless_detection_with_env_var(reset_spark_session):
    """Test that serverless is detected when DATABRICKS_SERVERLESS env var is set."""
    databricks_conf = {
        "run_name": "test",
        "new_cluster": {"spark_version": "13.3.x-scala2.12"},  # Has cluster config
    }

    @task(
        task_config=DatabricksV2(
            databricks_conf=databricks_conf,
            databricks_instance="test.cloud.databricks.com",
        )
    )
    def my_task(a: int) -> int:
        return a

    # Without env var, should NOT be serverless (has new_cluster)
    assert my_task._is_databricks_serverless() is False

    # With env var set, should BE serverless
    os.environ["DATABRICKS_SERVERLESS"] = "true"
    try:
        assert my_task._is_databricks_serverless() is True
    finally:
        del os.environ["DATABRICKS_SERVERLESS"]


def test_databricks_v2_serverless_detection_with_config(reset_spark_session):
    """Test that serverless is detected based on DatabricksV2 config."""
    # Serverless config: has environment_key, no cluster config
    serverless_conf = {
        "run_name": "serverless-test",
        "environment_key": "my-env",
    }

    @task(
        task_config=DatabricksV2(
            databricks_conf=serverless_conf,
            databricks_instance="test.cloud.databricks.com",
        )
    )
    def serverless_task(a: int) -> int:
        return a

    # Should detect serverless from config
    assert serverless_task._is_databricks_serverless() is True

    # Classic config: has new_cluster
    classic_conf = {
        "run_name": "classic-test",
        "new_cluster": {"spark_version": "13.3.x-scala2.12"},
    }

    @task(
        task_config=DatabricksV2(
            databricks_conf=classic_conf,
            databricks_instance="test.cloud.databricks.com",
        )
    )
    def classic_task(a: int) -> int:
        return a

    # Should NOT detect serverless
    assert classic_task._is_databricks_serverless() is False


def test_databricks_v2_serverless_detection_with_environments_array(reset_spark_session):
    """Test serverless detection with inline environments array."""
    serverless_conf = {
        "run_name": "serverless-inline",
        "environments": [{
            "environment_key": "default",
            "spec": {"client": "1", "dependencies": ["pandas"]}
        }],
    }

    @task(
        task_config=DatabricksV2(
            databricks_conf=serverless_conf,
            databricks_instance="test.cloud.databricks.com",
        )
    )
    def serverless_task(a: int) -> int:
        return a

    assert serverless_task._is_databricks_serverless() is True


def test_databricks_v2_classic_not_detected_as_serverless(reset_spark_session):
    """Test that classic compute is not incorrectly detected as serverless."""
    # Classic with existing_cluster_id
    existing_cluster_conf = {
        "run_name": "existing-cluster",
        "existing_cluster_id": "abc-123",
    }

    @task(
        task_config=DatabricksV2(
            databricks_conf=existing_cluster_conf,
            databricks_instance="test.cloud.databricks.com",
        )
    )
    def existing_cluster_task(a: int) -> int:
        return a

    assert existing_cluster_task._is_databricks_serverless() is False

    # Classic with new_cluster AND environment_key (cluster takes precedence)
    mixed_conf = {
        "run_name": "mixed",
        "new_cluster": {"spark_version": "13.3.x-scala2.12"},
        "environment_key": "my-env",  # Should be ignored
    }

    @task(
        task_config=DatabricksV2(
            databricks_conf=mixed_conf,
            databricks_instance="test.cloud.databricks.com",
        )
    )
    def mixed_task(a: int) -> int:
        return a

    assert mixed_task._is_databricks_serverless() is False


def test_databricks_v2_service_credential_provider():
    """Test that service credential provider is properly serialized."""
    serverless_conf = {
        "run_name": "serverless-with-creds",
        "environment_key": "my-env",
    }

    @task(
        task_config=DatabricksV2(
            databricks_conf=serverless_conf,
            databricks_instance="test.cloud.databricks.com",
            databricks_service_credential_provider="my-credential-provider",
        )
    )
    def task_with_creds(a: int) -> int:
        return a

    default_img = Image(name="default", fqn="test", tag="tag")
    settings = SerializationSettings(
        project="project",
        domain="domain",
        version="version",
        env={},
        image_config=ImageConfig(default_image=default_img, images=[default_img]),
    )

    custom = task_with_creds.get_custom(settings)
    assert custom.get("databricksServiceCredentialProvider") == "my-credential-provider"


def test_databricks_v2_no_service_credential_provider():
    """Test that custom dict doesn't have credential provider when not set."""
    serverless_conf = {
        "run_name": "serverless-no-creds",
        "environment_key": "my-env",
    }

    @task(
        task_config=DatabricksV2(
            databricks_conf=serverless_conf,
            databricks_instance="test.cloud.databricks.com",
        )
    )
    def task_no_creds(a: int) -> int:
        return a

    default_img = Image(name="default", fqn="test", tag="tag")
    settings = SerializationSettings(
        project="project",
        domain="domain",
        version="version",
        env={},
        image_config=ImageConfig(default_image=default_img, images=[default_img]),
    )

    custom = task_no_creds.get_custom(settings)
    assert "databricksServiceCredentialProvider" not in custom


def test_spark_classic_not_affected_by_serverless_code(reset_spark_session):
    """Test that regular Spark tasks (non-Databricks) are not affected by serverless code."""
    @task(
        task_config=Spark(
            spark_conf={"spark.driver.memory": "512M"},
        )
    )
    def spark_task(a: int) -> int:
        return a

    # Regular Spark task should NOT be detected as serverless
    assert spark_task._is_databricks_serverless() is False

    # pre_execute should work normally
    pb = ExecutionParameters.new_builder()
    pb.working_dir = "/tmp"
    pb.execution_id = "ex:local:local:local"
    p = pb.build()
    new_p = spark_task.pre_execute(p)
    
    assert new_p is not None
    assert new_p.has_attr("SPARK_SESSION")
    assert spark_task.sess is not None
