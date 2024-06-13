from enum import Enum
from typing import Callable, Optional

from kubernetes.client.models import (
    V1Container,
    V1ContainerPort,
    V1PodSpec,
    V1ResourceRequirements,
)

from flytekit import FlyteContextManager, PodTemplate
from flytekit.core.utils import ClassDecorator

INFERENCE_TYPE_VALUE = "model-inference"


class Cloud(Enum):
    AWS = "aws"
    GCP = "gcp"


class ModelInferenceTemplate(ClassDecorator):
    CLOUD = "cloud"
    INSTANCE = "instance"
    IMAGE = "image"
    PORT = "port"

    def __init__(
        self,
        port: int,
        cpu: int,
        gpu: int,
        mem: str,
        task_function: Optional[Callable] = None,
        cloud: Optional[Cloud] = None,
        image: Optional[str] = None,
        health_endpoint: str = "/",
        **init_kwargs: dict,
    ):
        self.cloud = cloud
        self.image = image
        self.port = port
        self.cpu = cpu
        self.gpu = gpu
        self.mem = mem
        self.health_endpoint = health_endpoint
        self.pod_template = PodTemplate()
        self.device = task_function.accelerator.device if task_function.accelerator else None

        super().__init__(task_function, **init_kwargs)
        self.update_pod_template()

    def update_pod_template(self):
        self.pod_template.pod_spec = V1PodSpec(
            init_containers=[
                V1Container(
                    name="model-server",
                    image=self.image,
                    ports=[V1ContainerPort(container_port=self.port)],
                    resources=V1ResourceRequirements(
                        requests={
                            "cpu": self.cpu,
                            "nvidia.com/gpu": self.gpu,
                            "memory": self.mem,
                        },
                        limits={
                            "cpu": self.cpu,
                            "nvidia.com/gpu": self.gpu,
                            "memory": self.mem,
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
                        f"until wget -qO- http://localhost:{self.port}/{self.health_endpoint}; do sleep 1; done;",
                    ],
                    resources=V1ResourceRequirements(
                        requests={"cpu": 1, "memory": "100Mi"},
                        limits={"cpu": 1, "memory": "100Mi"},
                    ),
                ),
            ],
        )

        if self.cloud == Cloud.AWS and self.device:
            self.pod_template.pod_spec.node_selector = {"k8s.amazonaws.com/accelerator": self.device}
        elif self.cloud == Cloud.GCP and self.device:
            self.pod_template.pod_spec.node_selector = {"cloud.google.com/gke-accelerator": self.device}

    def execute(self, *args, **kwargs):
        ctx = FlyteContextManager.current_context()
        is_local_execution = ctx.execution_state.is_local_execution()

        if is_local_execution:
            raise ValueError("Inference in a sidecar service doesn't work locally.")

        # Set the task function's pod template
        self.task_function.pod_template = self.pod_template

        output = self.task_function(*args, **kwargs)
        return output

    def get_extra_config(self):
        return {
            self.LINK_TYPE_KEY: INFERENCE_TYPE_VALUE,
            self.CLOUD: self.cloud.value,
            self.INSTANCE: self.device,
            self.IMAGE: self.image,
            self.PORT: str(self.port),
        }
