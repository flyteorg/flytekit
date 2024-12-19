from dataclasses import dataclass
from typing import Optional

from ..sidecar_template import ModelInferenceTemplate


@dataclass
class HFSecret:
    """
    :param secrets_prefix: The secrets prefix that Flyte appends to all mounted secrets.
    :param hf_token_group: The group name for the HuggingFace token.
    :param hf_token_key: The key name for the HuggingFace token.
    """

    secrets_prefix: str  # _UNION_ or _FSEC_
    hf_token_key: str
    hf_token_group: Optional[str] = None


class VLLM(ModelInferenceTemplate):
    def __init__(
        self,
        hf_secret: HFSecret,
        arg_dict: Optional[dict] = None,
        image: str = "vllm/vllm-openai",
        health_endpoint: str = "/health",
        port: int = 8000,
        cpu: int = 2,
        gpu: int = 1,
        mem: str = "10Gi",
    ):
        """
        Initialize NIM class for managing a Kubernetes pod template.

        :param hf_secret: Instance of HFSecret for managing hugging face secrets.
        :param arg_dict: A dictionary of arguments for the VLLM model server (https://docs.vllm.ai/en/stable/models/engine_args.html).
        :param image: The Docker image to be used for the model server container. Default is "vllm/vllm-openai".
        :param health_endpoint: The health endpoint for the model server container. Default is "/health".
        :param port: The port number for the model server container. Default is 8000.
        :param cpu: The number of CPU cores requested for the model server container. Default is 2.
        :param gpu: The number of GPU cores requested for the model server container. Default is 1.
        :param mem: The amount of memory requested for the model server container. Default is "10Gi".
        """
        if hf_secret.hf_token_key is None:
            raise ValueError("HuggingFace token key must be provided.")
        if hf_secret.secrets_prefix is None:
            raise ValueError("Secrets prefix must be provided.")

        self._hf_secret = hf_secret
        self._arg_dict = arg_dict

        super().__init__(
            image=image,
            health_endpoint=health_endpoint,
            port=port,
            cpu=cpu,
            gpu=gpu,
            mem=mem,
        )

        self.setup_vllm_pod_template()

    def setup_vllm_pod_template(self):
        from kubernetes.client.models import V1EnvVar

        model_server_container = self.pod_template.pod_spec.init_containers[0]

        if self._hf_secret.hf_token_group:
            hf_key = f"$({self._hf_secret.secrets_prefix}{self._hf_secret.hf_token_group}_{self._hf_secret.hf_token_key})".upper()
        else:
            hf_key = f"$({self._hf_secret.secrets_prefix}{self._hf_secret.hf_token_key})".upper()

        model_server_container.env = [
            V1EnvVar(name="HUGGING_FACE_HUB_TOKEN", value=hf_key),
        ]
        model_server_container.args = self.build_vllm_args()

    def build_vllm_args(self) -> list:
        args = []
        if self._arg_dict:
            for key, value in self._arg_dict.items():
                args.append(f"--{key}")
                if value is not None:
                    args.append(str(value))
        return args
