from flytekitplugins.spark import Spark
from mock import MagicMock, patch

from flytekit import task
from flytekit.remote.remote import FlyteRemote


@patch("flytekit.configuration.platform.URL")
@patch("flytekit.configuration.platform.INSECURE")
def test_spark_template_with_remote(mock_insecure, mock_url):
    @task(task_config=Spark(spark_conf={"spark": "1"}))
    def my_spark(a: str) -> int:
        return 10

    mock_url.get.return_value = "localhost"

    mock_insecure.get.return_value = True
    mock_client = MagicMock()

    remote = FlyteRemote.from_config("p1", "d1")

    remote._image_config = MagicMock()
    remote._client = mock_client

    remote.register(my_spark)
    serialized_spec = mock_client.create_task.call_args.kwargs["task_spec"]

    # Check if the serialized task has mainApplicaitonFile field set.
    assert serialized_spec.template.custom["mainApplicationFile"]
    assert serialized_spec.template.custom["sparkConf"]
