import datetime
import tempfile

import pytest

from flytekit import task, workflow
from flytekit.configuration import ImageConfig, SerializationSettings
from flytekit.core.task import TaskMetadata
from flytekit.sensor.base_sensor import BaseSensor
from flytekit.sensor.file_sensor import FileSensor
from tests.flytekit.unit.test_translator import default_img


def test_sensor_task():
    sensor = FileSensor(name="test_sensor")
    assert sensor.task_type == "sensor"
    settings = SerializationSettings(
        project="project",
        domain="domain",
        version="version",
        env={"FOO": "baz"},
        image_config=ImageConfig(default_image=default_img, images=[default_img]),
    )
    assert sensor.get_custom(settings) == {
        "sensor_module": "flytekit.sensor.file_sensor",
        "sensor_name": "FileSensor",
        "sensor_config": None,
        "inputs": None,
    }
    tmp_file = tempfile.NamedTemporaryFile()

    @task()
    def t1():
        print("flyte")

    @workflow
    def wf():
        sensor(tmp_file.name) >> t1()

    if __name__ == "__main__":
        wf()


def test_base_sensor_timeout_initialization():
    # Test with no timeout
    sensor = BaseSensor(name="test_sensor")
    assert sensor.metadata.timeout is None

    # Test with integer timeout (converted to timedelta internally)
    sensor = BaseSensor(name="test_sensor", timeout=60)
    assert sensor.metadata.timeout == datetime.timedelta(seconds=60)

    # Test with timedelta timeout
    timeout = datetime.timedelta(minutes=5)
    sensor = BaseSensor(name="test_sensor", timeout=timeout)
    assert sensor.metadata.timeout == timeout


def test_file_sensor_timeout_initialization():
    # Test with no timeout
    sensor = FileSensor(name="test_file_sensor")
    assert sensor.metadata.timeout is None

    # Test with integer timeout (converted to timedelta internally)
    sensor = FileSensor(name="test_file_sensor", timeout=60)
    assert sensor.metadata.timeout == datetime.timedelta(seconds=60)

    # Test with timedelta timeout
    timeout = datetime.timedelta(minutes=5)
    sensor = FileSensor(name="test_file_sensor", timeout=timeout)
    assert sensor.metadata.timeout == timeout


def test_agent_executor_timeout_logging():
    # Create a sensor with timeout
    sensor = BaseSensor(name="test_sensor", timeout=60)

    # Verify the timeout is set correctly (converted to timedelta internally)
    assert sensor.metadata.timeout == datetime.timedelta(seconds=60)

def test_file_sensor_set_timeout_and_metadata_at_the_same_time():
    """
    The test is the combination of the following cases:
    1. Set timeout and metadata with timeout at the same time
    2. Set timeout and metadata without timeout
    3. Set timeout and no metadata
    4. Not set timeout and metadata with timeout
    """

    timeout = datetime.timedelta(seconds=60)

    with pytest.raises(ValueError, match="You cannot set both timeout and metadata parameters at the same time in the sensor"):
        FileSensor(name="test_sensor", timeout=60, metadata=TaskMetadata(timeout=timeout))

    FileSensor(name="test_sensor", timeout=60, metadata=TaskMetadata())
    FileSensor(name="test_sensor", timeout=60)
    FileSensor(name="test_sensor", metadata=TaskMetadata(timeout=timeout))
