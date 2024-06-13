from typing import Callable, Optional

from kubernetes.client.models import (
    V1EmptyDirVolumeSource,
    V1EnvVar,
    V1EnvVarSource,
    V1LocalObjectReference,
    V1SecretKeySelector,
    V1SecurityContext,
    V1Volume,
    V1VolumeMount,
)

from flytekit import Secret

from ..sidecar_template import Cloud, ModelInferenceTemplate


class nim(ModelInferenceTemplate):
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
            health_endpoint="/v1/health/ready",
            **init_kwargs,
        )

        self.update_pod_template()

    def update_pod_template(self):
        super().update_pod_template()

        self.pod_template.pod_spec.volumes = [
            V1Volume(
                name="dshm",
                empty_dir=V1EmptyDirVolumeSource(medium="Memory", size_limit=self.shm_size),
            )
        ]
        self.pod_template.pod_spec.image_pull_secrets = [V1LocalObjectReference(name=self.nvcr_secret)]

        # Update the init containers with the additional environment variables
        model_server_container = self.pod_template.pod_spec.init_containers[0]
        model_server_container.env = [
            V1EnvVar(
                name="NGC_API_KEY",
                value_from=V1EnvVarSource(
                    secret_key_ref=V1SecretKeySelector(
                        name=self.ngc_secret.group,
                        key=self.ngc_secret.key,
                    )
                ),
            )
        ]
        model_server_container.volume_mounts = [V1VolumeMount(name="dshm", mount_path="/dev/shm")]
        model_server_container.security_context = V1SecurityContext(run_as_user=1000)

        self.task_function.secret_requests.append(self.ngc_secret)
