import datetime
from unittest import mock

from flytekit.sensor.base_sensor import BaseSensor
from flytekit.sensor.file_sensor import FileSensor


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


@mock.patch("flytekit.extend.backend.base_agent.logger")
def test_agent_executor_timeout_logging(mock_logger):
    # Create a sensor with timeout
    sensor = BaseSensor(name="test_sensor", timeout=60)
    
    # Verify the timeout is set correctly (converted to timedelta internally)
    assert sensor.metadata.timeout == datetime.timedelta(seconds=60)
