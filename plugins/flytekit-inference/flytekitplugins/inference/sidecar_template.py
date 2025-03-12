from typing import Optional

from flytekit import PodTemplate
from flytekit.configuration.default_images import DefaultImages


class ModelInferenceTemplate:
    def __init__(
        self,
        image: Optional[str] = None,
        health_endpoint: Optional[str] = None,
        port: int = 8000,
        cpu: int = 1,
        gpu: int = 1,
        mem: str = "1Gi",
        env: Optional[dict[str, str]] = None,
        download_inputs: bool = False,
        download_inputs_mem: str = "500Mi",
        download_inputs_cpu: int = 2,
    ):
        from kubernetes.client.models import (
            V1Container,
            V1ContainerPort,
            V1EnvVar,
            V1HTTPGetAction,
            V1PodSpec,
            V1Probe,
            V1ResourceRequirements,
            V1Volume,
            V1VolumeMount,
        )

        self._image = image
        self._health_endpoint = health_endpoint
        self._port = port
        self._cpu = cpu
        self._gpu = gpu
        self._mem = mem
        self._download_inputs_mem = download_inputs_mem
        self._download_inputs_cpu = download_inputs_cpu
        self._env = env
        self._download_inputs = download_inputs

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
                    startup_probe=(
                        V1Probe(
                            http_get=V1HTTPGetAction(
                                path=self._health_endpoint,
                                port=self._port,
                            ),
                            failure_threshold=100,  # The model server initialization can take some time, so the failure threshold is increased to accommodate this delay.
                        )
                        if self._health_endpoint
                        else None
                    ),
                ),
            ],
            volumes=[
                V1Volume(name="shared-data", empty_dir={}),
                V1Volume(name="tmp", empty_dir={}),
            ],
        )

        if self._download_inputs:
            input_download_code = """
import os
import json
import sys

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

            tmp_destination = None
            if not downloaded_file.startswith('/tmp'):
                tmp_destination = '/tmp' + os.path.basename(downloaded_file)
                shutil.copy(downloaded_file, tmp_destination)

            inputs[var_name] = tmp_destination or downloaded_file

with open('/shared/inputs.json', 'w') as f:
    json.dump(inputs, f)
"""

            self._pod_template.pod_spec.init_containers.append(
                V1Container(
                    name="input-downloader",
                    image=DefaultImages.default_image(),
                    command=["/bin/sh", "-c"],
                    args=[f'python3 -c "{input_download_code}" {{{{.input}}}}'],
                    volume_mounts=[
                        V1VolumeMount(name="shared-data", mount_path="/shared"),
                        V1VolumeMount(name="tmp", mount_path="/tmp"),
                    ],
                    resources=V1ResourceRequirements(
                        requests={
                            "cpu": self._download_inputs_cpu,
                            "memory": self._download_inputs_mem,
                        },
                        limits={
                            "cpu": self._download_inputs_cpu,
                            "memory": self._download_inputs_mem,
                        },
                    ),
                ),
            )

    @property
    def pod_template(self):
        return self._pod_template

    @property
    def base_url(self):
        return f"http://localhost:{self._port}"
