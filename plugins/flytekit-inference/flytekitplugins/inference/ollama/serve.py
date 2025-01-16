from dataclasses import dataclass
from typing import Any, Mapping, Optional, Sequence, Union

from ..sidecar_template import ModelInferenceTemplate


@dataclass
class Model:
    """Represents the configuration for a model used in a Kubernetes pod template.

    :param name: The name of the model.
    :param mem: The amount of memory allocated for the model, specified as a string. Default is "500Mi".
    :param cpu: The number of CPU cores allocated for the model. Default is 1.
    :param from: The name of an existing model to create the new model from.
    :param files: A list of file names to create the model from.
    :param adapters: A list of file names to create the model for LORA adapters.
    :param template: The prompt template for the model.
    :param license: A string or list of strings containing the license or licenses for the model.
    :param system: A string containing the system prompt for the model.
    :param parameters: A dictionary of parameters for the model.
    :param messages: A list of message objects used to create a conversation.
    :param quantize: Quantize a non-quantized (e.g. float16) model.
    """

    name: str
    mem: str = "500Mi"
    cpu: int = 1
    from_: Optional[str] = None
    files: Optional[list[str]] = None
    adapters: Optional[list[str]] = None
    template: Optional[str] = None
    license: Optional[Union[str, list[str]]] = None
    system: Optional[str] = None
    parameters: Optional[Mapping[str, Any]] = None
    messages: Optional[Sequence[Mapping[str, Any]]] = None
    quantize: Optional[str] = None


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

        :param model: An instance of the Model class containing the model's configuration, including its name, memory, CPU, and the modelfile parameters.
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
        self._model_from = model.from_
        self._model_files = model.files
        self._model_adapters = model.adapters
        self._model_template = model.template
        self._model_license = model.license
        self._model_system = model.system
        self._model_parameters = model.parameters
        self._model_messages = model.messages
        self._model_quantize = model.quantize

        super().__init__(
            image=image,
            port=port,
            cpu=cpu,
            gpu=gpu,
            mem=mem,
            download_inputs_mem=download_inputs_mem,
            download_inputs_cpu=download_inputs_cpu,
            download_inputs=bool(self._model_adapters or self._model_files),
        )

        self.setup_ollama_pod_template()

    def setup_ollama_pod_template(self):
        from kubernetes.client.models import (
            V1Container,
            V1ResourceRequirements,
            V1SecurityContext,
            V1VolumeMount,
        )

        custom_model = any(
            [
                self._model_files,
                self._model_adapters,
                self._model_template,
                self._model_license,
                self._model_system,
                self._model_parameters,
                self._model_messages,
                self._model_quantize,
            ]
        )
        container_name = "create-model" if custom_model else "pull-model"

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
        if custom_model:
            if self._model_files or self._model_adapters:
                python_code = f"""
{base_code}
import json
from ollama._client import Client

with open('/shared/inputs.json', 'r') as f:
    inputs = json.load(f)

files = {{}}
adapters = {{}}
client = Client('{self.base_url}')

for input_name, input_value in inputs.items():
    if input_name in {self._model_files}:
        files[input_name] = client.create_blob(input_value)
    if input_name in {self._model_adapters}:
        adapters[input_name] = client.create_blob(input_value)

{ollama_service_ready}

# Debugging: Shows the status of model creation.
for chunk in ollama.create(
    model={"'" + self._model_name + "'" if self._model_name is not None else None},
    from_={"'" + self._model_from + "'" if self._model_from is not None else None},
    files=files if files else None,
    adapters=adapters if adapters else None,
    template={"'" + self._model_template.replace("\n", "\\n") + "'" if self._model_template is not None else None},
    license={"'" + self._model_license + "'" if self._model_license is not None else None},
    system={"'" + self._model_system.replace("\n", "\\n") + "'" if self._model_system is not None else None},
    parameters={self._model_parameters if self._model_parameters is not None else None},
    messages={self._model_messages if self._model_messages is not None else None},
    quantize={"'" + self._model_quantize + "'" if self._model_quantize is not None else None},
    stream=True
):
    print(chunk)
"""
            else:
                python_code = f"""
{base_code}

{ollama_service_ready}

# Debugging: Shows the status of model creation.
for chunk in ollama.create(
    model={"'" + self._model_name + "'" if self._model_name is not None else None},
    from_={"'" + self._model_from + "'" if self._model_from is not None else None},
    files=None,
    adapters=None,
    template={"'" + self._model_template.replace("\n", "\\n") + "'" if self._model_template is not None else None},
    license={"'" + self._model_license + "'" if self._model_license is not None else None},
    system={"'" + self._model_system.replace("\n", "\\n") + "'" if self._model_system is not None else None},
    parameters={self._model_parameters if self._model_parameters is not None else None},
    messages={self._model_messages if self._model_messages is not None else None},
    quantize={"'" + self._model_quantize + "'" if self._model_quantize is not None else None},
    stream=True
):
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
                args=[
                    "apt-get update && apt-get install -y git && "
                    f"pip install requests && pip install git+https://github.com/ollama/ollama-python.git@eefe5c9666e2fa82ab17618155dd0aae47bba8fa && {command}"
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
                volume_mounts=[
                    V1VolumeMount(name="shared-data", mount_path="/shared"),
                    V1VolumeMount(name="tmp", mount_path="/tmp"),
                ],
            )
        )
