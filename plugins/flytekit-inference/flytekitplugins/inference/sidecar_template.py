from typing import Optional

from flytekit import PodTemplate


class ModelInferenceTemplate:
    def __init__(
        self,
        image: Optional[str] = None,
        health_endpoint: str = "/",
        port: int = 8000,
        cpu: int = 1,
        gpu: int = 1,
        mem: str = "1Gi",
        env: Optional[
            dict[str, str]
        ] = None,  # https://docs.nvidia.com/nim/large-language-models/latest/configuration.html#environment-variables
    ):
        from kubernetes.client.models import (
            V1Container,
            V1ContainerPort,
            V1EnvVar,
            V1HTTPGetAction,
            V1PodSpec,
            V1Probe,
            V1ResourceRequirements,
        )

        self._image = image
        self._health_endpoint = health_endpoint
        self._port = port
        self._cpu = cpu
        self._gpu = gpu
        self._mem = mem
        self._env = env

        self._pod_template = PodTemplate()

        if env and not isinstance(env, dict):
            raise ValueError("env must be a dict.")

        self._pod_template.pod_spec = V1PodSpec(
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
                    env=([V1EnvVar(name=k, value=v) for k, v in self._env.items()] if self._env else None),
                    startup_probe=V1Probe(
                        http_get=V1HTTPGetAction(path=self._health_endpoint, port=self._port),
                        failure_threshold=100,  # The model server initialization can take some time, so the failure threshold is increased to accommodate this delay.
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
