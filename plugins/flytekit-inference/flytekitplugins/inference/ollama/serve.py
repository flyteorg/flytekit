import base64
from dataclasses import dataclass
from typing import Optional

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
        download_inputs_mem: str = "500Mi",
        download_inputs_cpu: int = 2,
    ):
        """Initialize Ollama class for managing a Kubernetes pod template.

        :param model: An instance of the Model class containing the model's configuration, including its name, memory, CPU, and file.
        :param image: The Docker image to be used for the container. Default is "ollama/ollama".
        :param port: The port number on which the container should expose its service. Default is 11434.
        :param cpu: The number of CPU cores requested for the container. Default is 1.
        :param gpu: The number of GPUs requested for the container. Default is 1.
        :param mem: The amount of memory requested for the container, specified as a string. Default is "15Gi".
        :param download_inputs_mem: The amount of memory requested for downloading inputs, specified as a string. Default is "500Mi".
        :param download_inputs_cpu: The number of CPU cores requested for downloading inputs. Default is 2.
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
            download_inputs_mem=download_inputs_mem,
            download_inputs_cpu=download_inputs_cpu,
            download_inputs=(True if self._model_modelfile and "{inputs" in self._model_modelfile else False),
        )

        self.setup_ollama_pod_template()

    def setup_ollama_pod_template(self):
        from kubernetes.client.models import (
            V1Container,
            V1ResourceRequirements,
            V1SecurityContext,
            V1VolumeMount,
        )

        container_name = "create-model" if self._model_modelfile else "pull-model"

        base_code = """
import base64
import time
import ollama
import requests
"""

        ollama_service_ready = f"""
# Wait for Ollama service to be ready
max_retries = 30
retry_interval = 1
for _ in range(max_retries):
    try:
        response = requests.get('{self.base_url}')
        if response.status_code == 200:
            print('Ollama service is ready')
            break
    except requests.RequestException:
        pass
    time.sleep(retry_interval)
else:
    print('Ollama service did not become ready in time')
    exit(1)
"""
        if self._model_modelfile:
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

{ollama_service_ready}

# Debugging: Shows the status of model creation.
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

{ollama_service_ready}

# Debugging: Shows the status of model creation.
for chunk in ollama.create(model='{self._model_name}', path='Modelfile', stream=True):
    print(chunk)
"""
        else:
            python_code = f"""
{base_code}

{ollama_service_ready}

# Debugging: Shows the status of model pull.
for chunk in ollama.pull('{self._model_name}', stream=True):
    print(chunk)
"""

        command = f'python3 -c "{python_code}"'

        self.pod_template.pod_spec.init_containers.append(
            V1Container(
                name=container_name,
                image="python:3.11-slim",
                command=["/bin/sh", "-c"],
                args=[f"pip install requests && pip install ollama==0.3.3 && {command}"],
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
                volume_mounts=[
                    V1VolumeMount(name="shared-data", mount_path="/shared"),
                    V1VolumeMount(name="tmp", mount_path="/tmp"),
                ],
            )
        )
