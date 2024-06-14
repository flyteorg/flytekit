from typing import Optional

from kubernetes.client.models import (
    V1Container,
    V1ContainerPort,
    V1PodSpec,
    V1ResourceRequirements,
)

from flytekit import PodTemplate


class ModelInferenceTemplate:
    def __init__(
        self,
        node_selector: Optional[dict] = None,
        image: Optional[str] = None,
        health_endpoint: str = "/",
        port: int = 8000,
        cpu: int = 1,
        gpu: int = 1,
        mem: str = "1Gi",
        **init_kwargs: dict,
    ):
        self._node_selector = node_selector
        self._image = image
        self._health_endpoint = health_endpoint
        self._port = port
        self._cpu = cpu
        self._gpu = gpu
        self._mem = mem

        self._pod_template = PodTemplate()

        self.update_pod_template()

    def update_pod_template(self):
        self._pod_template.pod_spec = V1PodSpec(
            node_selector=self._node_selector,
            containers=[],
            init_containers=[
                V1Container(
                    name="model-server",
                    image=self._image,
                    ports=[V1ContainerPort(container_port=self._port)],
                    resources=V1ResourceRequirements(
                        requests={
                            "cpu": self._cpu,
                            "nvidia.com/gpu": self._gpu,
                            "memory": self._mem,
                        },
                        limits={
                            "cpu": self._cpu,
                            "nvidia.com/gpu": self._gpu,
                            "memory": self._mem,
                        },
                    ),
                    restart_policy="Always",  # treat this container as a sidecar
                ),
                V1Container(
                    name="wait-for-model-server",
                    image="busybox",
                    command=[
                        "sh",
                        "-c",
                        f"until wget -qO- http://localhost:{self._port}/{self._health_endpoint}; do sleep 1; done;",
                    ],
                    resources=V1ResourceRequirements(
                        requests={"cpu": 1, "memory": "100Mi"},
                        limits={"cpu": 1, "memory": "100Mi"},
                    ),
                ),
            ],
        )

    @property
    def pod_template(self):
        return self._pod_template

    @property
    def base_url(self):
        return f"http://localhost:{self._port}"
