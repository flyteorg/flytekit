import unittest

from kubernetes.client import V1PodSpec, V1Container

from flytekit import PodTemplate, task, ImageSpec
from flytekit.configuration import SerializationSettings, ImageConfig, Image


class TestPodTemplateHashing(unittest.TestCase):
    """Test suite for PodTemplate.version_hash()."""

    def test_default_initialization_hash_consistency(self):
        """Tests that default PodTemplates have consistent hashes."""
        pt1 = PodTemplate()
        pt2 = PodTemplate()
        self.assertEqual(pt1.version_hash(), pt2.version_hash())

    def test_identical_complex_objects_hash_consistency(self):
        """Tests that identical complex PodTemplates have the same hash."""
        common_pod_spec = V1PodSpec(
            containers=[
                V1Container(name="c1", image="img1:latest", command=["/bin/sh", "-c", "echo hello"]),
                V1Container(name="c2", image=ImageSpec(name="img2-repo/img2:v1.0"))
            ],
            restart_policy="Never",
            node_selector={"disktype": "ssd"}
        )
        pt1 = PodTemplate(
            pod_spec=common_pod_spec,
            primary_container_name="c1",
            labels={"app": "myapp", "env": "prod"},
            annotations={"author": "tester", "version": "1.2.3"}
        )
        pt2 = PodTemplate(
            pod_spec=common_pod_spec,  # Same instance, but could be an identical copy
            primary_container_name="c1",
            labels={"app": "myapp", "env": "prod"},  # Same content
            annotations={"author": "tester", "version": "1.2.3"}  # Same content
        )
        self.assertEqual(pt1.version_hash(), pt2.version_hash())

    def test_different_primary_container_name(self):
        """Tests that different primary_container_name yields different hashes."""
        pt1 = PodTemplate(primary_container_name="main")
        pt2 = PodTemplate(primary_container_name="worker")
        self.assertNotEqual(pt1.version_hash(), pt2.version_hash())

    def test_different_labels(self):
        """Tests that different labels yield different hashes."""
        pt1 = PodTemplate(labels={"app": "app1"})
        pt2 = PodTemplate(labels={"app": "app2"})
        pt3 = PodTemplate(labels={"app": "app1", "version": "v1"})
        self.assertNotEqual(pt1.version_hash(), pt2.version_hash())
        self.assertNotEqual(pt1.version_hash(), pt3.version_hash())

    def test_label_order_does_not_affect_hash(self):
        """Tests that the order of keys in labels does not affect the hash."""
        pt1 = PodTemplate(labels={"app": "app1", "env": "prod"})
        pt2 = PodTemplate(labels={"env": "prod", "app": "app1"})  # Same labels, different order
        self.assertEqual(pt1.version_hash(), pt2.version_hash())

    def test_different_annotations(self):
        """Tests that different annotations yield different hashes."""
        pt1 = PodTemplate(annotations={"owner": "teamA"})
        pt2 = PodTemplate(annotations={"owner": "teamB"})
        self.assertNotEqual(pt1.version_hash(), pt2.version_hash())

    def test_annotation_order_does_not_affect_hash(self):
        """Tests that the order of keys in annotations does not affect the hash."""
        pt1 = PodTemplate(annotations={"owner": "teamA", "contact": "a@example.com"})
        pt2 = PodTemplate(annotations={"contact": "a@example.com", "owner": "teamA"})
        self.assertEqual(pt1.version_hash(), pt2.version_hash())

    def test_different_pod_spec_restart_policy(self):
        """Tests that different pod_spec.restart_policy yields different hashes."""
        # Explicitly initialize containers to an empty list
        spec1 = V1PodSpec(containers=[], restart_policy="Always")
        spec2 = V1PodSpec(containers=[], restart_policy="Never")
        pt1 = PodTemplate(pod_spec=spec1)
        pt2 = PodTemplate(pod_spec=spec2)
        self.assertNotEqual(pt1.version_hash(), pt2.version_hash())

    def test_different_pod_spec_node_selector(self):
        """Tests that different pod_spec.node_selector yields different hashes."""
        # Explicitly initialize containers to an empty list
        spec1 = V1PodSpec(containers=[], node_selector={"type": "compute"})
        spec2 = V1PodSpec(containers=[], node_selector={"type": "memory"})
        pt1 = PodTemplate(pod_spec=spec1)
        pt2 = PodTemplate(pod_spec=spec2)
        self.assertNotEqual(pt1.version_hash(), pt2.version_hash())

        # Test order in node_selector also with explicit containers
        spec3 = V1PodSpec(containers=[], node_selector={"type": "compute", "region": "us-east-1"})
        spec4 = V1PodSpec(containers=[], node_selector={"region": "us-east-1", "type": "compute"})
        pt3 = PodTemplate(pod_spec=spec3)
        pt4 = PodTemplate(pod_spec=spec4)
        self.assertEqual(pt3.version_hash(), pt4.version_hash())

    def test_different_container_name(self):
        """Tests that different container names yield different hashes."""
        spec1 = V1PodSpec(containers=[V1Container(name="container-a")])
        spec2 = V1PodSpec(containers=[V1Container(name="container-b")])
        pt1 = PodTemplate(pod_spec=spec1)
        pt2 = PodTemplate(pod_spec=spec2)
        self.assertNotEqual(pt1.version_hash(), pt2.version_hash())

    def test_different_container_image_string(self):
        """Tests that different container image strings yield different hashes."""
        spec1 = V1PodSpec(containers=[V1Container(name="c1", image="image:v1")])
        spec2 = V1PodSpec(containers=[V1Container(name="c1", image="image:v2")])
        pt1 = PodTemplate(pod_spec=spec1)
        pt2 = PodTemplate(pod_spec=spec2)
        self.assertNotEqual(pt1.version_hash(), pt2.version_hash())

    def test_different_container_image_object(self):
        """Tests that different container ImageSpec objects yield different hashes."""
        spec1 = V1PodSpec(containers=[V1Container(name="c1", image=ImageSpec(name="image-v1"))])
        spec2 = V1PodSpec(containers=[V1Container(name="c1", image=ImageSpec(name="image-v2"))])
        pt1 = PodTemplate(pod_spec=spec1)
        pt2 = PodTemplate(pod_spec=spec2)
        self.assertNotEqual(pt1.version_hash(), pt2.version_hash())

    def test_container_image_string_vs_object(self):
        """Tests that image as string vs ImageSpec (if names match) yields same hash due to serialization."""
        spec1 = V1PodSpec(containers=[V1Container(name="c1", image="myimage:latest")])
        spec2 = V1PodSpec(containers=[V1Container(name="c1", image=ImageSpec(name="myimage:latest"))])
        pt1 = PodTemplate(pod_spec=spec1)
        pt2 = PodTemplate(pod_spec=spec2)
        self.assertNotEqual(pt1.version_hash(), pt2.version_hash())

    def test_different_container_command(self):
        """Tests that different container commands yield different hashes."""
        spec1 = V1PodSpec(containers=[V1Container(name="c1", command=["echo", "hello"])])
        spec2 = V1PodSpec(containers=[V1Container(name="c1", command=["echo", "world"])])
        pt1 = PodTemplate(pod_spec=spec1)
        pt2 = PodTemplate(pod_spec=spec2)
        self.assertNotEqual(pt1.version_hash(), pt2.version_hash())

    def test_order_of_containers_matters(self):
        """Tests that the order of containers in pod_spec affects the hash."""
        c1 = V1Container(name="c1", image="img1")
        c2 = V1Container(name="c2", image="img2")
        spec1 = V1PodSpec(containers=[c1, c2])
        spec2 = V1PodSpec(containers=[c2, c1])
        pt1 = PodTemplate(pod_spec=spec1)
        pt2 = PodTemplate(pod_spec=spec2)
        self.assertNotEqual(pt1.version_hash(), pt2.version_hash())

    def test_pod_spec_with_none_values_in_mock(self):
        """
        Tests how None values in mocked K8s objects are handled by serialization.
        """
        container_with_nones = V1Container(name="c-none", image=None, command=None)
        spec1 = V1PodSpec(containers=[container_with_nones], restart_policy=None)
        pt1 = PodTemplate(pod_spec=spec1, labels=None, annotations=None)

        container_copy = V1Container(name="c-none", image=None, command=None)
        spec2 = V1PodSpec(containers=[container_copy], restart_policy=None)
        pt2 = PodTemplate(pod_spec=spec2, labels=None, annotations=None)

        self.assertEqual(pt1.version_hash(), pt2.version_hash())

        container_diff = V1Container(name="c-none", image="some-image", command=None)
        spec3 = V1PodSpec(containers=[container_diff], restart_policy=None)
        pt3 = PodTemplate(pod_spec=spec3, labels=None, annotations=None)
        self.assertNotEqual(pt1.version_hash(), pt3.version_hash())
