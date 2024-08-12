from ..sidecar_template import ModelInferenceTemplate


class Ollama(ModelInferenceTemplate):
    def __init__(
        self,
        image: str = "ollama/ollama",
        port: int = 11434,
        cpu: int = 1,
        gpu: int = 1,
        server_mem: str = "15Gi",
        model: str = "llama3:8b-instruct-fp16",
        model_mem: str = "10Gi",
        model_cpu: str = 1,
        model_gpu: str = 0,
    ):
        self._model = model
        self._model_mem = model_mem
        self._model_cpu = model_cpu
        self._model_gpu = model_gpu

        super().__init__(image=image, port=port, cpu=cpu, gpu=gpu, mem=server_mem)

        self.setup_ollama_pod_template()

    def setup_ollama_pod_template(self):
        from kubernetes.client.models import (
            V1Container,
            V1ResourceRequirements,
        )

        self.pod_template.pod_spec.init_containers.append(
            V1Container(
                name="pull-model",
                image="curlimages/curl",
                command=[
                    "/bin/sh",
                    "-c",
                    f"sleep 15; curl -X POST {self.base_url}/api/pull -d '{{\"name\": \"{self._model}\"}}' -H 'Content-Type: application/json'",
                ],
                resources=V1ResourceRequirements(
                    requests={
                        "cpu": self._model_cpu,
                        "nvidia.com/gpu": self._model_gpu,
                        "memory": self._model_mem,
                    },
                    limits={
                        "cpu": self._model_cpu,
                        "nvidia.com/gpu": self._model_gpu,
                        "memory": self._model_mem,
                    },
                ),
            )
        )
