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

from flytekit.extras.accelerators import GPUAccelerator

from ..sidecar_template import Cloud, ModelInferenceTemplate


class nim(ModelInferenceTemplate):
    def __init__(
        self,
        task_function: Optional[Callable] = None,
        cloud: Optional[Cloud] = None,
        device: Optional[GPUAccelerator] = None,
        image: str = "nvcr.io/nim/meta/llama3-8b-instruct:1.0.0",
        port: int = 8000,
        cpu: int = 1,
        gpu: int = 1,
        mem: str = "20Gi",
        shm_size: str = "16Gi",
        ngc_image_secret: Optional[str] = None,
        ngc_secret_group: Optional[str] = None,
        ngc_secret_key: Optional[str] = None,
        health_endpoint: str = "v1/health/ready",
        **init_kwargs: dict,
    ):
        if ngc_image_secret is None:
            raise ValueError("NGC image pull credentials must be provided.")
        if ngc_secret_group is None:
            raise ValueError("NGC secret group must be provided.")
        if ngc_secret_key is None:
            raise ValueError("NGC secret key must be provided.")
        if not isinstance(cloud, Cloud):
            raise ValueError("cloud should derive from Cloud enum. Import Cloud from flytekitplugns.nim")
        if not isinstance(device, GPUAccelerator):
            raise ValueError("device must be a GPUAccelerator instance.")

        self.shm_size = shm_size
        self.ngc_image_secret = ngc_image_secret
        self.ngc_secret_group = ngc_secret_group
        self.ngc_secret_key = ngc_secret_key
        self.health_endpoint = health_endpoint

        # All kwargs need to be passed up so that the function wrapping works for both `@nim` and `@nim(...)`
        super().__init__(
            task_function,
            cloud=cloud,
            device=device,
            image=image,
            health_endpoint=health_endpoint,
            port=port,
            cpu=cpu,
            gpu=gpu,
            mem=mem,
            shm_size=shm_size,
            ngc_image_secret=ngc_image_secret,
            ngc_secret_group=ngc_secret_group,
            ngc_secret_key=ngc_secret_key,
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
        self.pod_template.pod_spec.image_pull_secrets = [V1LocalObjectReference(name=self.ngc_image_secret)]

        # Update the init containers with the additional environment variables
        model_server_container = self.pod_template.pod_spec.init_containers[0]
        model_server_container.env = [
            V1EnvVar(
                name="NGC_API_KEY",
                value_from=V1EnvVarSource(
                    secret_key_ref=V1SecretKeySelector(
                        name=self.ngc_secret_group,
                        key=self.ngc_secret_key,
                    )
                ),
            )
        ]
        model_server_container.volume_mounts = [V1VolumeMount(name="dshm", mount_path="/dev/shm")]
        model_server_container.security_context = V1SecurityContext(run_as_user=1000)
