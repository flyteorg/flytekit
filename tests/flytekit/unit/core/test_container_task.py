from kubernetes.client.models import (
    V1Affinity,
    V1NodeAffinity,
    V1NodeSelectorRequirement,
    V1NodeSelectorTerm,
    V1PodSpec,
    V1PreferredSchedulingTerm,
    V1Toleration,
)

from flytekit import kwtypes
from flytekit.configuration import Image, ImageConfig, SerializationSettings
from flytekit.core.container_task import ContainerTask
from flytekit.core.pod_template import PodTemplate
from flytekit.tools.translator import get_serializable_task


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
    ts = get_serializable_task(default_serialization_settings, ct)
    assert ts.template.metadata.pod_template_name == "my-base-template"
    assert ts.template.container is None
    assert ts.template.k8s_pod is not None
    serialized_pod_spec = ts.template.k8s_pod.pod_spec
    assert serialized_pod_spec["affinity"]["nodeAffinity"] is not None
    assert serialized_pod_spec["tolerations"] == [
        {"effect": "NoSchedule", "key": "nvidia.com/gpu", "operator": "Exists"}
    ]
    assert serialized_pod_spec["runtimeClassName"] == "nvidia"
