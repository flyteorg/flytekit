import unittest
from unittest.mock import MagicMock, patch
from flytekitplugins.k8sdataservice.k8s.manager import K8sManager, DEFAULT_RESOURCES
from kubernetes.client import V1ResourceRequirements
from kubernetes.client.rest import ApiException
from kubernetes.client import V1DeleteOptions
from flytekit import logger


class TestK8sManager(unittest.TestCase):

    def setUp(self):
        self.k8s_manager = K8sManager()
        self.k8s_manager.set_configs({
            "Cluster": "ei-dev2",
            "Name": "test-name",
            "Image": "test-image",
            "Command": ["echo", "hello"],
            "Replicas": 1,
            "Limits": {"cpu": "2", "memory": "4G"},
            "Requests": {"cpu": "1", "memory": "2G"},
            "ProxyAs": "test"
        })

    @patch("flytekitplugins.k8sdataservice.k8s.manager.K8sManager.create_service", return_value="test-service")
    @patch("flytekitplugins.k8sdataservice.k8s.manager.K8sManager.create_stateful_set", return_value="test-statefulset")
    def test_create_data_service(self, mock_create_stateful_set, mock_create_service):
        response = self.k8s_manager.create_data_service("test-kk-id")
        mock_create_service.assert_called_once()
        mock_create_stateful_set.assert_called_once()
        self.assertEqual(response, "test-statefulset")

    @patch("flytekitplugins.k8sdataservice.k8s.manager.client.AppsV1Api.create_namespaced_stateful_set")
    def test_create_stateful_set(self, mock_create_namespaced_stateful_set):
        mock_metadata = MagicMock()
        mock_metadata.name = "test-statefulset"
        mock_create_namespaced_stateful_set.return_value = MagicMock(metadata=mock_metadata)
        stateful_set_object = self.k8s_manager.create_stateful_set_object()
        response = self.k8s_manager.create_stateful_set(stateful_set_object)
        self.assertEqual(response, "test-statefulset")

    @patch("flytekitplugins.k8sdataservice.k8s.manager.client.AppsV1Api.create_namespaced_stateful_set")
    def test_create_stateful_set_failure(self, mock_create_namespaced_stateful_set):
        mock_create_namespaced_stateful_set.side_effect = ApiException("Create failed")
        stateful_set_object = self.k8s_manager.create_stateful_set_object()
        response = self.k8s_manager.create_stateful_set(stateful_set_object)
        self.assertEqual(response, "failed_stateful_set_name")

    @patch("flytekitplugins.k8sdataservice.k8s.manager.client.CoreV1Api.create_namespaced_service")
    def test_create_service(self, mock_create_namespaced_service):
        # Set the return value of the mock to simulate the actual response
        mock_metadata = MagicMock()
        mock_metadata.name = "test-service"
        mock_create_namespaced_service.return_value = MagicMock(metadata=mock_metadata)
        # Call the method and assert the result
        response = self.k8s_manager.create_service()
        self.assertEqual(response, "test-service")

    @patch("flytekitplugins.k8sdataservice.k8s.manager.client.CoreV1Api.create_namespaced_service")
    @patch("flytekitplugins.k8sdataservice.k8s.manager.logger")
    def test_create_service_exception(self, mock_logger, mock_create_namespaced_service):
        mock_create_namespaced_service.side_effect = ApiException("Failed to create service")
        with self.assertRaises(ApiException):
            self.k8s_manager.create_service()
        mock_logger.error.assert_called_once()
        logged_message = mock_logger.error.call_args[0][0]
        self.assertIn("Exception when calling CoreV1Api->create_namespaced_service: (Failed to create service)", logged_message)

    @patch("flytekitplugins.k8sdataservice.k8s.manager.client.AppsV1Api.read_namespaced_stateful_set")
    def test_check_stateful_set_status(self, mock_read_namespaced_stateful_set):
        mock_read_namespaced_stateful_set.return_value.status = MagicMock(replicas=1, available_replicas=1)
        self.assertEqual(self.k8s_manager.check_stateful_set_status("test-name"), "success")

        mock_read_namespaced_stateful_set.return_value.status = MagicMock(replicas=1, available_replicas=0)
        self.assertEqual(self.k8s_manager.check_stateful_set_status("test-name"), "running")

        mock_read_namespaced_stateful_set.return_value.status = MagicMock(replicas=0, available_replicas=0)
        self.assertEqual(self.k8s_manager.check_stateful_set_status("test-name"), "pending")

    @patch("flytekitplugins.k8sdataservice.k8s.manager.client.AppsV1Api.delete_namespaced_stateful_set")
    def test_delete_stateful_set(self, mock_delete_stateful_set):
        self.k8s_manager.namespace = "kk-flyte-dev2"
        self.k8s_manager.delete_stateful_set("test-name")
        mock_delete_stateful_set.assert_called_once_with(name="test-name", namespace="kk-flyte-dev2", body=V1DeleteOptions())

    @patch("flytekitplugins.k8sdataservice.k8s.manager.client.CoreV1Api.delete_namespaced_service")
    def test_delete_service(self, mock_delete_service):
        self.k8s_manager.namespace = "kk-flyte-dev2"
        self.k8s_manager.delete_service("test-name")
        mock_delete_service.assert_called_once_with(name="test-name", namespace="kk-flyte-dev2", body=V1DeleteOptions())

    def test_get_resources(self):
        resources = self.k8s_manager.get_resources()
        assert self.k8s_manager.data_service_config.get("Limits") is not None
        self.assertIsInstance(resources, V1ResourceRequirements)
        self.assertEqual(resources.limits, {"cpu": "2", "memory": "4G"})
        self.assertEqual(resources.requests, {"cpu": "1", "memory": "2G"})

    @patch("flytekitplugins.k8sdataservice.k8s.manager.client.AppsV1Api.read_namespaced_stateful_set")
    @patch("flytekitplugins.k8sdataservice.k8s.manager.logger")
    def test_check_stateful_set_status_exception(self, mock_logger, mock_read_namespaced_stateful_set):
        mock_read_namespaced_stateful_set.side_effect = ApiException("Failed to read StatefulSet")
        result = self.k8s_manager.check_stateful_set_status("test-statefulset")
        expected_message = "Error checking status of StatefulSet test-statefulset: (Failed to read StatefulSet)"
        self.assertIn(expected_message, result)
        mock_logger.error.assert_called_once()
        logged_message = mock_logger.error.call_args[0][0]
        self.assertIn("Exception when calling AppsV1Api->read_namespaced_stateful_set: (Failed to read StatefulSet)", logged_message)

    @patch("flytekitplugins.k8sdataservice.k8s.manager.client.AppsV1Api.read_namespaced_stateful_set")
    @patch("flytekitplugins.k8sdataservice.k8s.manager.logger")
    def test_check_stateful_set_status_unknown(self, mock_logger, mock_read_namespaced_stateful_set):
        mock_status = MagicMock()
        mock_status.replicas = 3
        mock_status.available_replicas = None
        mock_read_namespaced_stateful_set.return_value.status = mock_status
        result = self.k8s_manager.check_stateful_set_status("test-statefulset")
        self.assertEqual(result, "failed")
        mock_logger.info.assert_any_call("StatefulSet test-statefulset status is unknown. Replicas:  3, available: None")

    @patch("flytekitplugins.k8sdataservice.k8s.manager.client.CoreV1Api.delete_namespaced_service")
    @patch("flytekitplugins.k8sdataservice.k8s.manager.logger")
    def test_delete_service_exception(self, mock_logger, mock_delete_service):
        mock_delete_service.side_effect = ApiException("Failed to delete Service")
        self.k8s_manager.delete_service("test-service")
        mock_logger.error.assert_called_once()
        logged_message = mock_logger.error.call_args[0][0]
        self.assertIn("Exception when calling CoreV1Api->delete_namespaced_service: (Failed to delete Service)", logged_message)

    @patch("flytekitplugins.k8sdataservice.k8s.manager.client.AppsV1Api.delete_namespaced_stateful_set")
    @patch("flytekitplugins.k8sdataservice.k8s.manager.logger")
    def test_delete_stateful_set_exception(self, mock_logger, mock_delete_stateful_set):
        mock_delete_stateful_set.side_effect = ApiException("Failed to delete StatefulSet")
        self.k8s_manager.delete_stateful_set("test-statefulset")
        mock_logger.error.assert_called_once()
        logged_message = mock_logger.error.call_args[0][0]
        self.assertIn("Exception when calling AppsV1Api->delete_namespaced_stateful_set: (Failed to delete StatefulSet)", logged_message)

    def test_get_resources_default(self):
        self.k8s_manager.set_configs({
            "Cluster": "ei-dev2",
            "Name": "test-name",
            "Image": "test-image",
            "Command": ["echo", "hello"],
            "Replicas": 1,
            "Limits": {"cpu": "6", "memory": "16G"},
            "Requests": {"cpu": "2", "memory": "10G"},
            "ProxyAs": "test"
        })
        resources = self.k8s_manager.get_resources()
        logger.info(f"After Testing default resources: {DEFAULT_RESOURCES}")
        self.assertIsInstance(resources, V1ResourceRequirements)
        self.assertEqual(resources.limits, {"cpu": "6", "memory": "16G"})
        self.assertEqual(resources.requests, {"cpu": "2", "memory": "10G"})


class TestK8sManagerExecutionIDFromExisting(unittest.TestCase):

    def setUp(self):
        self.k8s_manager = K8sManager()
        self.k8s_manager.namespace = "test-namespace"

    @patch("flytekitplugins.k8sdataservice.k8s.manager.client.AppsV1Api.list_namespaced_stateful_set")
    @patch("flytekitplugins.k8sdataservice.k8s.manager.logger")
    def test_get_execution_id_from_existing_found(self, mock_logger, mock_list_stateful_set):
        mock_metadata = MagicMock()
        mock_metadata.labels = {"kong.linkedin.com/executionID": "test-execution-id"}
        mock_stateful_set = MagicMock()
        mock_stateful_set.metadata = mock_metadata
        mock_list_stateful_set.return_value.items = [mock_stateful_set]
        execution_id = self.k8s_manager.get_execution_id_from_existing("test-release-name")
        self.assertEqual(execution_id, "test-execution-id")
        mock_logger.info.assert_any_call("The kingkong execution ID is test-execution-id")

    @patch("flytekitplugins.k8sdataservice.k8s.manager.client.AppsV1Api.list_namespaced_stateful_set")
    @patch("flytekitplugins.k8sdataservice.k8s.manager.logger")
    def test_get_execution_id_from_existing_not_found(self, mock_logger, mock_list_stateful_set):
        mock_list_stateful_set.return_value.items = []
        execution_id = self.k8s_manager.get_execution_id_from_existing("test-release-name")
        self.assertIsNone(execution_id)
        mock_logger.info.assert_called_once_with("There is no existing data service")

    @patch("flytekitplugins.k8sdataservice.k8s.manager.client.AppsV1Api.list_namespaced_stateful_set")
    @patch("flytekitplugins.k8sdataservice.k8s.manager.logger")
    def test_get_execution_id_from_existing_api_exception(self, mock_logger, mock_list_stateful_set):
        mock_list_stateful_set.side_effect = ApiException("API call failed")
        execution_id = self.k8s_manager.get_execution_id_from_existing("test-release-name")
        self.assertIsNone(execution_id)
        mock_logger.error.assert_called_once()
        logged_message = mock_logger.error.call_args[0][0]
        self.assertIn("Exception when calling AppsV1Api->list_namespaced_stateful_set:", logged_message)
        self.assertIn("API call failed", logged_message)
