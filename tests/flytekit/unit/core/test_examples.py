from collections import OrderedDict

from kubernetes.client.models import (
    V1Container,
    V1PodSpec,
    V1ResourceRequirements,
)

import flytekit.configuration
from flytekit.configuration import Image, ImageConfig
from flytekit.core.environment import Environment
from flytekit.core.pod_template import PodTemplate
from flytekit.core.task import task
from flytekit.image_spec.image_spec import ImageSpec, ImageBuildEngine
from flytekit.tools.translator import get_serializable

default_img = Image(name="default", fqn="test", tag="tag")
serialization_settings = flytekit.configuration.SerializationSettings(
    project="project",
    domain="domain",
    version="version",
    env=None,
    image_config=ImageConfig(default_image=default_img, images=[default_img]),
)


def _helper_get_pod_template() -> PodTemplate:
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
                resources=V1ResourceRequirements(
                    requests={"cpu": "1", "memory": "100Mi"},
                    limits={"cpu": "1", "memory": "100Mi"},
                ),
            ),
        ]
    )

    pt = PodTemplate(pod_spec=ps, primary_container_name="primary", labels={"test_k": "test_v"})
    return pt


sample_pt = _helper_get_pod_template()


def test_normal_pod_template(mock_image_spec_builder):
    ImageBuildEngine.register("test", mock_image_spec_builder)

    @task(
        pod_template=sample_pt,
    )
    def t_normal():
        ...

    spec = get_serializable(OrderedDict(),serialization_settings, t_normal)
    assert spec.template.k8s_pod.metadata.labels == sample_pt.labels


def test_env_pod_template(mock_image_spec_builder):
    ImageBuildEngine.register("test", mock_image_spec_builder)

    env_pod_templated = Environment(pod_template=sample_pt)

    @env_pod_templated.task
    def t_using_env():
        ...

    spec = get_serializable(OrderedDict(), serialization_settings, t_using_env)
    assert spec.template.k8s_pod.metadata.labels == sample_pt.labels


def test_jkl(mock_image_spec_builder):
    ImageBuildEngine.register("test", mock_image_spec_builder)

    from functools import partial

    pt_task = partial(task, pod_template=sample_pt)

    @pt_task
    def t_using_partial():
        ...

    spec = get_serializable(OrderedDict(), serialization_settings, t_using_partial)
    assert spec.template.k8s_pod.metadata.labels == sample_pt.labels
