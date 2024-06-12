from enum import Enum
from typing import Callable, Optional

from kubernetes.client.models import (
    V1Container,
    V1ContainerPort,
    V1EmptyDirVolumeSource,
    V1EnvVar,
    V1EnvVarSource,
    V1LocalObjectReference,
    V1PodSpec,
    V1ResourceRequirements,
    V1SecretKeySelector,
    V1SecurityContext,
    V1Volume,
    V1VolumeMount,
)

from flytekit import FlyteContextManager, PodTemplate, Secret
from flytekit.core.utils import ClassDecorator


class Cloud(Enum):
    AWS = "aws"
    GCP = "gcp"


NIM_TYPE_VALUE = "nim"


class nim(ClassDecorator):
    NIM_CLOUD = "cloud"
    NIM_INSTANCE = "instance"
    NIM_IMAGE = "image"
    NIM_PORT = "port"

    def __init__(
        self,
        task_function: Optional[Callable] = None,
        cloud: Cloud = Cloud.AWS,
        image: str = "nvcr.io/nim/meta/llama3-8b-instruct:1.0.0",
        port: int = 8000,
        cpu: int = 1,
        gpu: int = 1,
        mem: str = "20Gi",
        shm_size: str = "16Gi",
        nvcr_image_secret: str = "nvcrio-cred",
        ngc_secret: Secret = Secret(group="ngc", key="api_key"),
        **init_kwargs: dict,
    ):
        self.cloud = cloud
        self.image = image
        self.port = port
        self.cpu = cpu
        self.gpu = gpu
        self.mem = mem
        self.shm_size = shm_size
        self.nvcr_secret = nvcr_image_secret
        self.ngc_secret = ngc_secret

        # All kwargs need to be passed up so that the function wrapping works for both `@nim` and `@nim(...)`
        super().__init__(
            task_function,
            cloud=cloud,
            image=image,
            port=port,
            cpu=cpu,
            gpu=gpu,
            mem=mem,
            shm_size=shm_size,
            nvcr_image_secret=nvcr_image_secret,
            **init_kwargs,
        )

    def execute(self, *args, **kwargs):
        ctx = FlyteContextManager.current_context()
        is_local_execution = ctx.execution_state.is_local_execution()

        if is_local_execution:
            raise ValueError("NIM doesn't work locally.")

        if self.cloud == Cloud.AWS:
            node_selector = {"k8s.amazonaws.com/accelerator": self.task_function.accelerator.device}
        elif self.cloud == Cloud.GCP:
            node_selector = {"cloud.google.com/gke-accelerator": self.task_function.accelerator.device}

        self.task_function.secret_requests.append(self.ngc_secret)

        pod_template = PodTemplate(
            pod_spec=V1PodSpec(
                node_selector=node_selector,
                init_containers=[
                    V1Container(
                        name="model-server",
                        image=self.image,
                        env=[
                            V1EnvVar(
                                name="NGC_API_KEY",
                                value_from=V1EnvVarSource(
                                    secret_key_ref=V1SecretKeySelector(
                                        name=self.ngc_secret.group,
                                        key=self.ngc_secret.key,
                                    )
                                ),
                            ),
                        ],
                        ports=[V1ContainerPort(container_port=8000)],
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
                        security_context=V1SecurityContext(run_as_user=1000),
                        volume_mounts=[V1VolumeMount(name="dshm", mount_path="/dev/shm")],
                        restart_policy="Always",  # treat this container as a sidecar
                    ),
                    V1Container(
                        name="wait-for-model-server",
                        image="busybox",
                        command=[
                            "sh",
                            "-c",
                            "until wget -qO- http://localhost:8000/v1/health/ready; do sleep 1; done;",
                        ],
                        resources=V1ResourceRequirements(
                            requests={"cpu": 1, "memory": "100Mi"},
                            limits={"cpu": 1, "memory": "100Mi"},
                        ),
                    ),
                ],
                volumes=[
                    V1Volume(
                        name="dshm",
                        empty_dir=V1EmptyDirVolumeSource(medium="Memory", size_limit=self.shm_size),
                    )
                ],
                image_pull_secrets=[V1LocalObjectReference(name=self.nvcr_image_secret)],
            ),
        )
        self.task_function.pod_template = pod_template

        output = self.task_function(*args, **kwargs)
        return output

    def get_extra_config(self):
        return {
            self.LINK_TYPE_KEY: NIM_TYPE_VALUE,
            self.NIM_CLOUD: self.cloud.value,
            self.NIM_INSTANCE: self.task_function.accelerator.device,
            self.NIM_IMAGE: self.image,
            self.NIM_PORT: str(self.port),
        }
