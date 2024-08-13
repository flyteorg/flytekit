from dataclasses import dataclass
from typing import Optional

from ..sidecar_template import ModelInferenceTemplate


@dataclass
class Model:
    name: str = "llama3:8b-instruct-fp16"
    mem: str = "500Mi"
    cpu: int = 1
    modelfile: Optional[str] = None


class Ollama(ModelInferenceTemplate):
    def __init__(
        self,
        *,
        image: str = "ollama/ollama",
        port: int = 11434,
        cpu: int = 1,
        gpu: int = 1,
        server_mem: str = "15Gi",
        model: Model = Model(),
    ):
        self._model_name = model.name
        self._model_mem = model.mem
        self._model_cpu = model.cpu
        self._model_modelfile = model.modelfile

        super().__init__(image=image, port=port, cpu=cpu, gpu=gpu, mem=server_mem)

        self.setup_ollama_pod_template()

    def setup_ollama_pod_template(self):
        from kubernetes.client.models import (
            V1Container,
            V1ResourceRequirements,
        )

        container_name = "create-model" if self._model_modelfile else "pull-model"
        command = (
            f'sleep 15; curl -X POST {self.base_url}/api/create -d \'{{"name": "{self._model_name}", "modelfile": "{self._model_modelfile}"}}\''
            if self._model_modelfile
            else f'sleep 15; curl -X POST {self.base_url}/api/pull -d \'{{"name": "{self._model_name}"}}\''
        )

        self.pod_template.pod_spec.init_containers.append(
            V1Container(
                name=container_name,
                image="curlimages/curl",
                command=["/bin/sh", "-c", command],
                resources=V1ResourceRequirements(
                    requests={
                        "cpu": self._model_cpu,
                        "memory": self._model_mem,
                    },
                    limits={
                        "cpu": self._model_cpu,
                        "memory": self._model_mem,
                    },
                ),
            )
        )
