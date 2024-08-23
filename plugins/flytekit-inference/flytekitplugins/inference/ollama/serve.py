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

        python_code = None
        no_inputs_python_code = None

        if self._model_modelfile:
            python_code = """import base64
import os
import json
import sys

import ollama
from flyteidl.core import literals_pb2 as _literals_pb2
from flytekit.core import utils
from flytekit.core.context_manager import FlyteContextManager
from flytekit.interaction.string_literals import literal_map_string_repr
from flytekit.models import literals as _literal_models
from flytekit.models.core.types import BlobType
from flytekit.types.file import FlyteFile

input_arg = sys.argv[-1]

ctx = FlyteContextManager.current_context()
local_inputs_file = os.path.join(ctx.execution_state.working_dir, 'inputs.pb')
ctx.file_access.get_data(
    input_arg,
    local_inputs_file,
)
input_proto = utils.load_proto_from_file(_literals_pb2.LiteralMap, local_inputs_file)
idl_input_literals = _literal_models.LiteralMap.from_flyte_idl(input_proto)

inputs = literal_map_string_repr(idl_input_literals)

for var_name, literal in idl_input_literals.literals.items():
    if literal.scalar and literal.scalar.blob:
        if (
            literal.scalar.blob.metadata.type.dimensionality
            == BlobType.BlobDimensionality.SINGLE
        ):
            downloaded_file = FlyteFile.from_source(literal.scalar.blob.uri).download()
            inputs[var_name] = downloaded_file

"""

            python_code += """
class AttrDict(dict):
    'Convert a dictionary to an attribute style lookup. Do not use this in regular places, this is used for namespacing inputs and outputs'

    def __init__(self, *args, **kwargs):
        super(AttrDict, self).__init__(*args, **kwargs)
        self.__dict__ = self


inputs = {'inputs': AttrDict(inputs)}

"""
            encoded_modelfile = base64.b64encode(self._model_modelfile.encode("utf-8")).decode("utf-8")

            python_code += f"""
encoded_model_file = '{encoded_modelfile}'

modelfile = base64.b64decode(encoded_model_file).decode('utf-8').format(**inputs)
modelfile = modelfile.replace('{{', '{{{{').replace('}}', '}}}}')

with open('Modelfile', 'w') as f:
    f.write(modelfile)

for chunk in ollama.create(model='{self._model_name}', path='Modelfile', stream=True):
    print(chunk)
"""

            no_inputs_python_code = f"""
import base64
import ollama

encoded_model_file = '{encoded_modelfile}'

modelfile = base64.b64decode(encoded_model_file).decode('utf-8')

with open('Modelfile', 'w') as f:
    f.write(modelfile)

for chunk in ollama.create(model='{self._model_name}', path='Modelfile', stream=True):
    print(chunk)
"""

        command = (
            f'sleep 15 && python3 -c "{python_code}" {{{{.input}}}}'
            if self._model_modelfile and "{inputs" in self._model_modelfile
            else (
                f'sleep 15 && python3 -c "{no_inputs_python_code}"'
                if self._model_modelfile and "{inputs" not in self._model_modelfile
                else f'sleep 15 && curl -X POST {self.base_url}/api/pull -d \'{{"name": "{self._model_name}"}}\''
            )
        )

        self.pod_template.pod_spec.init_containers.append(
            V1Container(
                name=container_name,
                image=DefaultImages.default_image(),
                command=["/bin/sh", "-c"],
                args=[f"apt-get install -y curl && pip install ollama && {command}"],
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
