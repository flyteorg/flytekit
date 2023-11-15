import typing

import pandas as pd
from flytekit import ImageSpec, Resources, task, workflow
from flytekit import PodTemplate
from kubernetes.client.models import (
    V1PodSpec,
    V1Container,
    V1ResourceRequirements
)

# Use Podtemplate with 2 containers with image spec


image_spec_1 = ImageSpec(
    name="image-1",
    packages=["numpy"],
    registry="localhost:30000",
)

image_spec_2 = ImageSpec(
    name="image-2",
    packages=["pandas"],
    registry="localhost:30000",
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
            )
        )
    ]
)

pt = PodTemplate(pod_spec=ps, primary_container_name="primary")



@task(
    pod_template=pt,
)
def t1():
    ...

@workflow
def wf():
    t1()

if __name__ == "__main__":
    ...
    # builder.build_image(pandas_image_spec)