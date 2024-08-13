"""Test functionality that is shared between the pytorch and pytorch-elastic tasks."""

from contextlib import nullcontext
from typing import Union

import pytest
from flytekitplugins.kfpytorch.task import Elastic, PyTorch
from kubernetes.client import V1Container, V1EmptyDirVolumeSource, V1PodSpec, V1Volume, V1VolumeMount

from flytekit import PodTemplate, task


@pytest.mark.parametrize(
    "task_config, pod_template, needs_shm_volume, raises",
    [
        # Test that by default shared memory volume is added
        (PyTorch(num_workers=3), None, True, False),
        (Elastic(nnodes=2, increase_shared_mem=True), None, True, False),
        # Test disabling shared memory volume
        (PyTorch(num_workers=3, increase_shared_mem=False), None, False, False),
        (Elastic(nnodes=2, increase_shared_mem=False), None, False, False),
        # Test that explicitly passed pod template does not break adding shm volume
        (Elastic(nnodes=2, increase_shared_mem=True), PodTemplate(), True, False),
        # Test that pod template with container does not break adding shm volume
        (
            Elastic(nnodes=2),
            PodTemplate(
                pod_spec=V1PodSpec(containers=[V1Container(name="primary")]),
            ),
            True,
            False,
        ),
        # Test that pod template with volume/volume mount does not break adding shm volume
        (
            Elastic(nnodes=2),
            PodTemplate(
                pod_spec=V1PodSpec(
                    containers=[
                        V1Container(name="primary", volume_mounts=[V1VolumeMount(name="foo", mount_path="/bar")])
                    ],
                    volumes=[V1Volume(name="foo")],
                ),
            ),
            True,
            False,
        ),
        # Test that pod template with multiple containers raises an error
        (
            Elastic(nnodes=2),
            PodTemplate(
                pod_spec=V1PodSpec(
                    containers=[
                        V1Container(name="primary"),
                        V1Container(name="secondary"),
                    ]
                ),
            ),
            True,
            True,
        ),
        # Test that explicitly configured pod template with shared memory volume is not removed if `increase_shared_mem=False`
        (
            Elastic(nnodes=2, increase_shared_mem=False),
            PodTemplate(
                pod_spec=V1PodSpec(
                    containers=[
                        V1Container(name="primary", volume_mounts=[V1VolumeMount(name="shm", mount_path="/dev/shm")]),
                    ],
                    volumes=[V1Volume(name="shm", empty_dir=V1EmptyDirVolumeSource(medium="Memory"))],
                ),
            ),
            True,
            False,
        ),
        # Test that we raise if the user explicitly configured a shared memory volume and still configures the task config to add it
        (
            Elastic(nnodes=2, increase_shared_mem=True),
            PodTemplate(
                pod_spec=V1PodSpec(
                    containers=[
                        V1Container(name="primary", volume_mounts=[V1VolumeMount(name="shm", mount_path="/dev/shm")]),
                    ],
                    volumes=[V1Volume(name="shm", empty_dir=V1EmptyDirVolumeSource(medium="Memory"))],
                ),
            ),
            True,
            True,
        ),
    ],
)
def test_task_shared_memory(
    task_config: Union[Elastic, PyTorch], pod_template: PodTemplate, needs_shm_volume: bool, raises: bool
):
    """Test that the task pod template is configured with a shared memory volume if needed."""

    expected_volume = V1Volume(name="shm", empty_dir=V1EmptyDirVolumeSource(medium="Memory"))
    expected_volume_mount = V1VolumeMount(name="shm", mount_path="/dev/shm")

    with pytest.raises(ValueError) if raises else nullcontext():

        @task(
            task_config=task_config,
            pod_template=pod_template,
        )
        def test_task() -> None:
            pass

        if needs_shm_volume:
            assert test_task.pod_template is not None
            assert test_task.pod_template.pod_spec is not None
            assert test_task.pod_template.pod_spec.volumes is not None
            assert test_task.pod_template.pod_spec.containers is not None
            assert test_task.pod_template.pod_spec.containers[0].volume_mounts is not None

            assert any([v == expected_volume for v in test_task.pod_template.pod_spec.volumes])
            assert any(
                [v == expected_volume_mount for v in test_task.pod_template.pod_spec.containers[0].volume_mounts]
            )

        else:
            # Check that the shared memory volume + volume mount is not added
            no_pod_template = test_task.pod_template is None
            no_pod_spec = no_pod_template or test_task.pod_template.pod_spec is None
            no_volumes = no_pod_spec or test_task.pod_template.pod_spec.volumes is None
            no_containers = no_pod_spec or len(test_task.pod_template.pod_spec.containers) == 0
            no_volume_mounts = no_containers or test_task.pod_template.pod_spec.containers[0].volume_mounts is None
            empty_volume_mounts = (
                no_volume_mounts or len(test_task.pod_template.pod_spec.containers[0].volume_mounts) == 0
            )
            no_shm_volume_condition = no_volumes or not any(
                [v == expected_volume for v in test_task.pod_template.pod_spec.volumes]
            )
            no_shm_volume_mount_condition = empty_volume_mounts or not any(
                [v == expected_volume_mount for v in test_task.pod_template.pod_spec.containers[0].volume_mounts]
            )

            assert no_shm_volume_condition
            assert no_shm_volume_mount_condition
