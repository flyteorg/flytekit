from flytekit import PodTemplate, task
from flytekit.configuration import SerializationSettings, ImageConfig, Image


def test_mutation():
    pod_template_before = PodTemplate(
        labels={"lKeyA": "lValA", "lKeyB": "lValB"},
    )

    pod_template_after = PodTemplate(
        labels={"lKeyA": "lValA", "lKeyB": "lValB"},
    )

    settings = SerializationSettings(image_config=ImageConfig(default_image=Image(
                name='default',
                fqn='cr.flyte.org/flyteorg/flytekit',
                tag='latest',
                digest=None
            )))

    @task(pod_template=pod_template_before)
    def my_task():
        pass

    my_task.get_k8s_pod(settings=settings)


    assert pod_template_before == pod_template_after
