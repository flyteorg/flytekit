import unittest
from kubernetes import client
from utils.resources import cleanup_resources, convert_flyte_to_k8s_fields


class TestCleanupResources(unittest.TestCase):

    def test_cleanup_resources(self):
        resources = client.V1ResourceRequirements(
            limits={"cpu": "2", "memory": "10G", "ephemeral_storage": "0", "storage": "0GB"},
            requests={"cpu": "1", "memory": "0", "ephemeral_storage": "5G", "storage": "0"}
        )
        cleanup_resources(resources)
        expected_limits = {"cpu": "2", "memory": "10G"}
        self.assertEqual(resources.limits, expected_limits)

        expected_requests = {"cpu": "1", "ephemeral_storage": "5G"}
        self.assertEqual(resources.requests, expected_requests)

    def test_cleanup_resources_no_zero_like_values(self):
        resources = client.V1ResourceRequirements(
            limits={"cpu": "4", "memory": "16G"},
            requests={"cpu": "2", "memory": "8G"}
        )

        cleanup_resources(resources)

        expected_limits = {"cpu": "4", "memory": "16G"}
        expected_requests = {"cpu": "2", "memory": "8G"}

        self.assertEqual(resources.limits, expected_limits)
        self.assertEqual(resources.requests, expected_requests)

    def test_cleanup_resources_all_zero_like_values(self):
        resources = client.V1ResourceRequirements(
            limits={"cpu": "0", "memory": "0GB", "ephemeral_storage": "0"},
            requests={"cpu": "0", "memory": "0", "ephemeral_storage": "0GB"}
        )

        cleanup_resources(resources)

        self.assertEqual(resources.limits, {})
        self.assertEqual(resources.requests, {})

    def test_cleanup_resources_none_value(self):
        resources = client.V1ResourceRequirements(
            limits={"cpu": "4", "memory": "16G", "ephemeral_storage": None},
            requests={"cpu": "2", "memory": "8", "storage": None}
        )

        cleanup_resources(resources)

        expected_limits = {"cpu": "4", "memory": "16G"}
        expected_requests = {"cpu": "2", "memory": "8"}

        self.assertEqual(resources.limits, expected_limits)
        self.assertEqual(resources.requests, expected_requests)


class TestConvertFlyteToK8sFields(unittest.TestCase):

    def test_convert_flyte_to_k8s_fields(self):
        input_dict = {
            "cpu": "2",
            "mem": "10G",
            "ephemeral_storage": "50G"
        }

        expected_output = {
            "cpu": "2",
            "memory": "10G",
            "ephemeral_storage": "50G"
        }

        result = convert_flyte_to_k8s_fields(input_dict)
        self.assertEqual(result, expected_output)

    def test_no_mem_key(self):
        input_dict = {
            "cpu": "1",
            "storage": "100G"
        }

        expected_output = {
            "cpu": "1",
            "storage": "100G"
        }

        result = convert_flyte_to_k8s_fields(input_dict)
        self.assertEqual(result, expected_output)
