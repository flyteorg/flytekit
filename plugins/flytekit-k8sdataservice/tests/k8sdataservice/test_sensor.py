import pytest
import asyncio
import unittest
from unittest.mock import patch, MagicMock
from kubernetes import client
from kubernetes.client.rest import ApiException
from flytekitplugins.k8sdataservice.sensor import CleanupSensor


@pytest.mark.asyncio
@patch("flytekitplugins.k8sdataservice.sensor.KubeConfig")
@patch("flytekitplugins.k8sdataservice.sensor.client.AppsV1Api")
@patch("flytekitplugins.k8sdataservice.sensor.client.CoreV1Api")
@patch("flytekitplugins.k8sdataservice.sensor.client.CustomObjectsApi")
@patch("flytekitplugins.k8sdataservice.sensor.logger")
async def test_poke_no_cleanup(
    mock_logger,
    mock_custom_api,
    mock_core_v1_api,
    mock_apps_v1_api,
    mock_kube_config
):
    mock_kube_config.return_value = MagicMock()
    sensor = CleanupSensor(name="test-sensor")
    await asyncio.create_task(
        sensor.poke(release_name="test-release",
                    cleanup_data_service=False,
                    cluster="test-cluster")
    )
    mock_kube_config.return_value.load_kube_config.assert_called_once()
    assert isinstance(sensor.apps_v1_api, MagicMock)
    assert isinstance(sensor.core_v1_api, MagicMock)
    assert isinstance(sensor.custom_api, MagicMock)
    assert sensor.release_name == "test-release"
    assert sensor.cleanup_data_service is False
    assert sensor.namespace == "flyte"
    assert sensor.cluster == "test-cluster"


@pytest.mark.asyncio
@patch("flytekitplugins.k8sdataservice.sensor.CleanupSensor.delete_data_service")
@patch("flytekitplugins.k8sdataservice.sensor.KubeConfig")
@patch("flytekitplugins.k8sdataservice.sensor.client.AppsV1Api")
@patch("flytekitplugins.k8sdataservice.sensor.client.CoreV1Api")
@patch("flytekitplugins.k8sdataservice.sensor.client.CustomObjectsApi")
@patch("flytekitplugins.k8sdataservice.sensor.logger")
async def test_poke_with_cleanup(
    mock_logger,
    mock_custom_api,
    mock_core_v1_api,
    mock_apps_v1_api,
    mock_kube_config,
    mock_delete_data_service
):
    mock_kube_config.return_value = MagicMock()
    # Initialize CleanupSensor instance
    sensor = CleanupSensor(name="test-sensor")

    async def sensor_poke_task():
        await sensor.poke(
            release_name="test-release",
            cleanup_data_service=True,
            cluster="test-cluster"
        )

    # Use asyncio.wait_for for timeout handling (compatible with older Python versions)
    try:
        await asyncio.wait_for(sensor_poke_task(), timeout=30)
    except asyncio.TimeoutError:
        pytest.fail("Test timed out")

    # Assertions for logger and delete_data_service call
    mock_logger.info.assert_any_call(
        "The training job is in terminal stage, deleting graph engine test-release"
    )
    mock_delete_data_service.assert_called_once()


class TestCleanupSensor(unittest.TestCase):
    @patch("flytekitplugins.k8sdataservice.sensor.logger")
    @patch("flytekitplugins.k8sdataservice.sensor.client.CoreV1Api")
    @patch("flytekitplugins.k8sdataservice.sensor.client.AppsV1Api")
    def test_delete_data_service(self, mock_apps_v1_api, mock_core_v1_api, mock_logger):
        # Configure mocks for service deletion
        mock_core_v1_api_instance = mock_core_v1_api.return_value
        mock_apps_v1_api_instance = mock_apps_v1_api.return_value
        # Initialize sensor and set required properties
        sensor = CleanupSensor(name="test-sensor")
        sensor.core_v1_api = mock_core_v1_api_instance
        sensor.apps_v1_api = mock_apps_v1_api_instance
        sensor.release_name = "test-release"
        sensor.namespace = "test-namespace"

        # Call delete_data_service
        sensor.delete_data_service()

        # Verify the service deletion calls
        mock_core_v1_api_instance.delete_namespaced_service.assert_called_once_with(
            name="test-release", namespace="test-namespace", body=client.V1DeleteOptions()
        )
        mock_apps_v1_api_instance.delete_namespaced_stateful_set.assert_called_once_with(
            name="test-release", namespace="test-namespace", body=client.V1DeleteOptions()
        )
        # Check for logger messages indicating success
        mock_logger.info.assert_any_call("Deleted Service: test-release")
        mock_logger.info.assert_any_call("Deleted StatefulSet: test-release")

    @patch("flytekitplugins.k8sdataservice.sensor.logger")
    @patch("flytekitplugins.k8sdataservice.sensor.client.CoreV1Api")
    @patch("flytekitplugins.k8sdataservice.sensor.client.AppsV1Api")
    def test_delete_data_service_with_exceptions(self, mock_apps_v1_api, mock_core_v1_api, mock_logger):
        # Configure mocks to raise exceptions
        mock_core_v1_api_instance = mock_core_v1_api.return_value
        mock_apps_v1_api_instance = mock_apps_v1_api.return_value

        mock_core_v1_api_instance.delete_namespaced_service.side_effect = ApiException("Service deletion failed")
        mock_apps_v1_api_instance.delete_namespaced_stateful_set.side_effect = ApiException("StatefulSet deletion failed")

        # Initialize sensor and set required properties
        sensor = CleanupSensor(name="test-sensor")
        sensor.core_v1_api = mock_core_v1_api_instance
        sensor.apps_v1_api = mock_apps_v1_api_instance
        sensor.release_name = "test-release"
        sensor.namespace = "test-namespace"

        # Call delete_data_service and handle exceptions
        sensor.delete_data_service()

        # Verify that errors were logged
        service_error_logged = any("Error deleting Service" in call[0][0] for call in mock_logger.error.call_args_list)
        statefulset_error_logged = any("Error deleting StatefulSet" in call[0][0] for call in mock_logger.error.call_args_list)

        self.assertTrue(service_error_logged, "Service deletion error not logged as expected")
        self.assertTrue(statefulset_error_logged, "StatefulSet deletion error not logged as expected")
