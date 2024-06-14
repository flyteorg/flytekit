from typing import Callable, Optional

from kubernetes.client.models import (
    V1Container,
    V1ContainerPort,
    V1PodSpec,
    V1ResourceRequirements,
)

from flytekit import FlyteContextManager, PodTemplate
from flytekit.core.utils import ClassDecorator


class ModelInferenceTemplate(ClassDecorator):
    NODE_SELECTOR = "node_selector"
    IMAGE = "image"
    PORT = "port"

    def __init__(
        self,
        task_function: Optional[Callable] = None,
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

        super().__init__(
            task_function,
            node_selector=node_selector,
            image=image,
            health_endpoint=health_endpoint,
            port=port,
            cpu=cpu,
            gpu=gpu,
            mem=mem,
            **init_kwargs,
        )
        self.update_pod_template()

    @property
    def pod_template(self):
        return self._pod_template

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

    def execute(self, *args, **kwargs):
        ctx = FlyteContextManager.current_context()
        is_local_execution = ctx.execution_state.is_local_execution()

        if is_local_execution:
            raise ValueError("Inference in a sidecar service doesn't work locally.")

        output = self.task_function(*args, **kwargs)
        return output

    def get_extra_config(self):
        return {
            self.NODE_SELECTOR: self._node_selector,
            self.IMAGE: self._image,
            self.PORT: self._port,
        }
