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
    :param modelfile: The actual model file as a JSON-serializable string. This represents the file content. Default is `None` if not applicable.
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
        health_endpoint: str = "/",
    ):
        """Initialize Ollama class for managing a Kubernetes pod template.

        :param model: An instance of the Model class containing the model's configuration, including its name, memory, CPU, and file.
        :param image: The Docker image to be used for the container. Default is "ollama/ollama".
        :param port: The port number on which the container should expose its service. Default is 11434.
        :param cpu: The number of CPU cores requested for the container. Default is 1.
        :param gpu: The number of GPUs requested for the container. Default is 1.
        :param mem: The amount of memory requested for the container, specified as a string. Default is "15Gi".
        :param health_endpoint: The health endpoint for the model server container. Default is "/".
        """
        self._model_name = model.name
        self._model_mem = model.mem
        self._model_cpu = model.cpu
        self._model_modelfile = model.modelfile

        super().__init__(
            image=image,
            port=port,
            cpu=cpu,
            gpu=gpu,
            mem=mem,
            health_endpoint=health_endpoint,
            download_inputs=(True if self._model_modelfile and "{inputs" in self._model_modelfile else False),
        )

        self.setup_ollama_pod_template()

    def setup_ollama_pod_template(self):
        from kubernetes.client.models import (
            V1Container,
            V1ResourceRequirements,
            V1VolumeMount,
        )

        container_name = "create-model" if self._model_modelfile else "pull-model"

        if self._model_modelfile:
            base_code = """
import base64
import time
import ollama
"""
            encoded_modelfile = base64.b64encode(self._model_modelfile.encode("utf-8")).decode("utf-8")

            if "{inputs" in self._model_modelfile:
                python_code = f"""
{base_code}
import json

with open('/shared/inputs.json', 'r') as f:
    inputs = json.load(f)

class AttrDict(dict):
    def __init__(self, *args, **kwargs):
        super(AttrDict, self).__init__(*args, **kwargs)
        self.__dict__ = self

inputs = {{'inputs': AttrDict(inputs)}}

encoded_model_file = '{encoded_modelfile}'

modelfile = base64.b64decode(encoded_model_file).decode('utf-8').format(**inputs)
modelfile = modelfile.replace('{{', '{{{{').replace('}}', '}}}}')

with open('Modelfile', 'w') as f:
    f.write(modelfile)

time.sleep(15)

for chunk in ollama.create(model='{self._model_name}', path='Modelfile', stream=True):
    print(chunk)
"""
            else:
                python_code = f"""
{base_code}

encoded_model_file = '{encoded_modelfile}'

modelfile = base64.b64decode(encoded_model_file).decode('utf-8')

with open('Modelfile', 'w') as f:
    f.write(modelfile)

time.sleep(15)

for chunk in ollama.create(model='{self._model_name}', path='Modelfile', stream=True):
    print(chunk)
"""
        else:
            python_code = f"""
{base_code}

time.sleep(15)

for chunk in ollama.pull('{self._model_name}', stream=True):
    print(chunk)
"""

        command = f'python3 -c "{python_code}"'

        self.pod_template.pod_spec.init_containers.append(
            V1Container(
                name=container_name,
                image=DefaultImages.default_image(),
                command=["/bin/sh", "-c"],
                args=[f"pip install ollama && {command}"],
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
                volume_mounts=[V1VolumeMount(name="shared-data", mount_path="/shared")],
            )
        )
