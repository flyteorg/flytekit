import os
import sys
import time
import docker

from collections import OrderedDict
from typing import Tuple
from datetime import timedelta

import pytest
from kubernetes.client.models import (
    V1Affinity,
    V1Container,
    V1NodeAffinity,
    V1NodeSelectorRequirement,
    V1NodeSelectorTerm,
    V1PodSpec,
    V1PreferredSchedulingTerm,
    V1ResourceRequirements,
    V1Toleration,
)

from flytekit import kwtypes, task, workflow
from flytekit.configuration import Image, ImageConfig, SerializationSettings
from flytekit.core.container_task import ContainerTask
from flytekit.core.pod_template import PodTemplate
from flytekit.image_spec.image_spec import ImageBuildEngine, ImageSpec
from flytekit.tools.translator import get_serializable_task


@pytest.mark.skipif(
    sys.platform in ["darwin", "win32"],
    reason="Skip if running on windows or macos due to CI Docker environment setup failure",
)
def test_local_execution():
    calculate_ellipse_area_python_template_style = ContainerTask(
        name="calculate_ellipse_area_python_template_style",
        input_data_dir="/var/inputs",
        output_data_dir="/var/outputs",
        inputs=kwtypes(a=float, b=float),
        outputs=kwtypes(area=float, metadata=str),
        image="ghcr.io/flyteorg/rawcontainers-python:v2",
        command=[
            "python",
            "calculate-ellipse-area.py",
            "{{.inputs.a}}",
            "{{.inputs.b}}",
            "/var/outputs",
        ],
    )

    area, metadata = calculate_ellipse_area_python_template_style(a=3.0, b=4.0)
    assert isinstance(area, float)
    assert isinstance(metadata, str)

    # Workflow execution with container task
    @task
    def t1(a: float, b: float) -> Tuple[float, float]:
        return a + b, a * b

    @workflow
    def wf(a: float, b: float) -> Tuple[float, str]:
        a, b = t1(a=a, b=b)
        area, metadata = calculate_ellipse_area_python_template_style(a=a, b=b)
        return area, metadata

    area, metadata = wf(a=3.0, b=4.0)
    assert isinstance(area, float)
    assert isinstance(metadata, str)


@pytest.mark.skipif(
    sys.platform == "win32",
    reason="Skip if running on windows due to path error",
)
def test_local_execution_special_cases():
    # Boolean conversion from string checks
    assert all([bool(s) for s in ["False", "false", "True", "true"]])

    # Path normalization
    input_data_dir = "/var/inputs"
    assert os.path.normpath(input_data_dir) == "/var/inputs"
    assert os.path.normpath(input_data_dir + "/") == "/var/inputs"

    # Datetime and timedelta string conversions
    ct = ContainerTask(
        name="local-execution",
        image="test-image",
        command="echo",
    )

    from datetime import datetime, timedelta

    now = datetime.now()
    assert datetime.fromisoformat(str(now)) == now
    td = timedelta(days=1, hours=1, minutes=1, seconds=1, microseconds=1)
    assert td == ct._string_to_timedelta(str(td))


def test_pod_template():
    ps = V1PodSpec(
        containers=[], tolerations=[V1Toleration(effect="NoSchedule", key="nvidia.com/gpu", operator="Exists")]
    )
    ps.runtime_class_name = "nvidia"
    nsr = V1NodeSelectorRequirement(key="nvidia.com/gpu.memory", operator="Gt", values=["10000"])
    pref_sched = V1PreferredSchedulingTerm(preference=V1NodeSelectorTerm(match_expressions=[nsr]), weight=1)
    ps.affinity = V1Affinity(
        node_affinity=V1NodeAffinity(preferred_during_scheduling_ignored_during_execution=[pref_sched])
    )
    pt = PodTemplate(pod_spec=ps, labels={"somelabel": "foobar"})

    image = "ghcr.io/flyteorg/rawcontainers-shell:v2"
    cmd = [
        "./calculate-ellipse-area.sh",
        "{{.inputs.a}}",
        "{{.inputs.b}}",
        "/var/outputs",
    ]
    ct = ContainerTask(
        name="ellipse-area-metadata-shell",
        input_data_dir="/var/inputs",
        output_data_dir="/var/outputs",
        inputs=kwtypes(a=float, b=float),
        outputs=kwtypes(area=float, metadata=str),
        image=image,
        command=cmd,
        pod_template=pt,
        pod_template_name="my-base-template",
    )

    assert ct.metadata.pod_template_name == "my-base-template"

    default_image = Image(name="default", fqn="docker.io/xyz", tag="some-git-hash")
    default_image_config = ImageConfig(default_image=default_image)
    default_serialization_settings = SerializationSettings(
        project="p", domain="d", version="v", image_config=default_image_config
    )

    container = ct.get_container(default_serialization_settings)
    assert container is None

    k8s_pod = ct.get_k8s_pod(default_serialization_settings)
    assert k8s_pod.metadata.labels == {"somelabel": "foobar"}

    primary_container = k8s_pod.pod_spec["containers"][0]

    assert primary_container["image"] == image
    assert primary_container["command"] == cmd

    #################
    # Test Serialization
    #################
    ts = get_serializable_task(OrderedDict(), default_serialization_settings, ct)
    assert ts.template.metadata.pod_template_name == "my-base-template"
    assert ts.template.container is None
    assert ts.template.k8s_pod is not None
    serialized_pod_spec = ts.template.k8s_pod.pod_spec
    assert serialized_pod_spec["affinity"]["nodeAffinity"] is not None
    assert serialized_pod_spec["tolerations"] == [
        {"effect": "NoSchedule", "key": "nvidia.com/gpu", "operator": "Exists"}
    ]
    assert serialized_pod_spec["runtimeClassName"] == "nvidia"


def test_raw_container_with_image_spec(mock_image_spec_builder):
    ImageBuildEngine.register("test-raw-container", mock_image_spec_builder)
    image_spec = ImageSpec(registry="flyte", base_image="r-base", builder="test-raw-container")

    calculate_ellipse_area_r = ContainerTask(
        name="ellipse-area-metadata-r",
        input_data_dir="/var/inputs",
        output_data_dir="/var/outputs",
        inputs=kwtypes(a=float, b=float),
        outputs=kwtypes(area=float, metadata=str),
        image=image_spec,
        command=[
            "Rscript",
            "--vanilla",
            "/root/calculate-ellipse-area.R",
            "{{.inputs.a}}",
            "{{.inputs.b}}",
            "/var/outputs",
        ],
    )

    default_serialization_settings = SerializationSettings(
        project="p", domain="d", version="v", image_config=ImageConfig.auto()
    )
    container = calculate_ellipse_area_r.get_container(default_serialization_settings)
    assert container.image == image_spec.image_name()


def test_container_task_image_spec(mock_image_spec_builder):
    default_image = Image(name="default", fqn="docker.io/xyz", tag="some-git-hash")
    default_image_config = ImageConfig(default_image=default_image)

    default_serialization_settings = SerializationSettings(
        project="p", domain="d", version="v", image_config=default_image_config, env={"FOO": "bar"}
    )

    image_spec_1 = ImageSpec(
        name="image-1",
        packages=["numpy"],
        registry="localhost:30000",
        builder="test",
    )

    image_spec_2 = ImageSpec(
        name="image-2",
        packages=["pandas"],
        registry="localhost:30000",
        builder="test",
    )

    ps = V1PodSpec(
        containers=[
            V1Container(
                name="primary",
                image=image_spec_1,
            ),
            V1Container(
                name="secondary",
                image=image_spec_2,
                # use 1 cpu and 1Gi mem
                resources=V1ResourceRequirements(
                    requests={"cpu": "1", "memory": "100Mi"},
                    limits={"cpu": "1", "memory": "100Mi"},
                ),
            ),
        ]
    )

    pt = PodTemplate(pod_spec=ps, primary_container_name="primary")

    ct = ContainerTask(
        name="x",
        image="ddd",
        command=["ccc"],
        pod_template=pt,
    )
    ImageBuildEngine.register("test", mock_image_spec_builder)
    pod = ct.get_k8s_pod(default_serialization_settings)
    assert pod.pod_spec["containers"][0]["image"] == image_spec_1.image_name()
    assert pod.pod_spec["containers"][1]["image"] == image_spec_2.image_name()

def test_container_task_timeout():
    ct_with_timeout = ContainerTask(
        name="timeout-test",
        image="busybox",
        command=["sleep", "5"],
        timeout=1,
    )

    with pytest.raises((docker.errors.APIError, Exception)):
        ct_with_timeout.execute()

    ct_with_timedelta = ContainerTask(
        name="timedelta-timeout-test",
        image="busybox",
        command=["sleep", "2"],
        timeout=timedelta(seconds=1),
    )

    with pytest.raises((docker.errors.APIError, Exception)):
        ct_with_timedelta.execute()


def test_container_task_timeout_k8s_serialization():
    from datetime import timedelta

    ps = V1PodSpec(
        containers=[], tolerations=[V1Toleration(effect="NoSchedule", key="nvidia.com/gpu", operator="Exists")]
    )
    pt = PodTemplate(pod_spec=ps, labels={"test": "timeout"})

    ct_numeric = ContainerTask(
        name="timeout-k8s-test",
        image="busybox",
        command=["echo", "hello"],
        pod_template=pt,
        timeout=60,
    )

    default_image = Image(name="default", fqn="docker.io/xyz", tag="some-git-hash")
    default_image_config = ImageConfig(default_image=default_image)
    default_serialization_settings = SerializationSettings(
        project="p", domain="d", version="v", image_config=default_image_config
    )

    k8s_pod = ct_numeric.get_k8s_pod(default_serialization_settings)
    assert k8s_pod.pod_spec["activeDeadlineSeconds"] == 60

    ct_timedelta = ContainerTask(
        name="timeout-k8s-timedelta-test",
        image="busybox",
        command=["echo", "hello"],
        pod_template=pt,
        timeout=timedelta(minutes=2),
    )

    k8s_pod_timedelta = ct_timedelta.get_k8s_pod(default_serialization_settings)
    assert k8s_pod_timedelta.pod_spec["activeDeadlineSeconds"] == 120
