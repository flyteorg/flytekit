import unittest
from unittest.mock import patch
from flytekitplugins.k8sdataservice.k8s.kube_config import KubeConfig
from kubernetes.config import ConfigException


class TestKubeConfig(unittest.TestCase):

    @patch("flytekitplugins.k8sdataservice.k8s.kube_config.config.load_incluster_config")
    def test_load_kube_config_success(self, mock_load_incluster_config):
        kube_config = KubeConfig()
        kube_config.load_kube_config()
        mock_load_incluster_config.assert_called_once()

    @patch("flytekitplugins.k8sdataservice.k8s.kube_config.config.load_incluster_config")
    def test_load_kube_config_failure(self, mock_load_incluster_config):
        # Simulate a ConfigException
        mock_load_incluster_config.side_effect = ConfigException("In-cluster config not found.")
        kube_config = KubeConfig()

        with self.assertLogs('flytekit', level='WARNING') as log:
            kube_config.load_kube_config()
            self.assertEqual(f"WARNING:flytekit:Failed to load in-cluster configuration. In-cluster config not found.", log.output[-1])
