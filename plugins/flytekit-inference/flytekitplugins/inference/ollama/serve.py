import base64
from dataclasses import dataclass
from typing import Optional

from flytekit.configuration.default_images import DefaultImages

from ..sidecar_template import ModelInferenceTemplate


@dataclass
class Model:
    """Represents the configuration for a model used in a Kubernetes pod template.

    :param name: The name of the model.
    :param mem: The amount of memory allocated for the model, specified as a string. Default is "500Mi".
    :param cpu: The number of CPU cores allocated for the model. Default is 1.
    :param modelfile: The actual model file as a string. This represents the file content. Default is `None` if not applicable.
    """

    name: str
    mem: str = "500Mi"
    cpu: int = 1
    modelfile: Optional[str] = None


class Ollama(ModelInferenceTemplate):
    def __init__(
        self,
        *,
        model: Model,
        image: str = "ollama/ollama",
        port: int = 11434,
        cpu: int = 1,
        gpu: int = 1,
        mem: str = "15Gi",
    ):
        """Initialize Ollama class for managing a Kubernetes pod template.

        :param model: An instance of the Model class containing the model's configuration, including its name, memory, CPU, and file.
        :param image: The Docker image to be used for the container. Default is "ollama/ollama".
        :param port: The port number on which the container should expose its service. Default is 11434.
        :param cpu: The number of CPU cores requested for the container. Default is 1.
        :param gpu: The number of GPUs requested for the container. Default is 1.
        :param mem: The amount of memory requested for the container, specified as a string (e.g., "15Gi" for 15 gigabytes). Default is "15Gi".
        """
        self._model_name = model.name
        self._model_mem = model.mem
        self._model_cpu = model.cpu
        self._model_modelfile = model.modelfile

        super().__init__(image=image, port=port, cpu=cpu, gpu=gpu, mem=mem)

        self.setup_ollama_pod_template()

    def setup_ollama_pod_template(self):
        from kubernetes.client.models import (
            V1Container,
            V1ResourceRequirements,
            V1SecurityContext,
        )

        container_name = "create-model" if self._model_modelfile else "pull-model"

        encoded_modelfile = base64.b64encode(self._model_modelfile.encode("utf-8")).decode("utf-8")

        command = (
            f'updated_modelfile=$(python3 download_inputs.py --encoded_modelfile "{encoded_modelfile}"); sleep 15; curl -X POST {self.base_url}/api/create -d \'{{"name": "{self._model_name}", "modelfile": "$updated_modelfile"}}\''
            if encoded_modelfile
            else f'sleep 15; curl -X POST {self.base_url}/api/pull -d \'{{"name": "{self._model_name}"}}\''
        )

        self.pod_template.pod_spec.init_containers.append(
            V1Container(
                name=container_name,
                image=DefaultImages.default_image(),
                command=[
                    "/bin/sh",
                    "-c",
                    f"apt-get install -y curl && {command}",
                ],
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
                security_context=V1SecurityContext(
                    run_as_user=0,
                ),
            )
        )
