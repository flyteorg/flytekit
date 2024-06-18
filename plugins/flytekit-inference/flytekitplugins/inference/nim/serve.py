from typing import Optional

from kubernetes.client.models import (
    V1Container,
    V1EmptyDirVolumeSource,
    V1EnvVar,
    V1EnvVarSource,
    V1LocalObjectReference,
    V1ResourceRequirements,
    V1SecretKeySelector,
    V1SecurityContext,
    V1Volume,
    V1VolumeMount,
)

from ..sidecar_template import ModelInferenceTemplate


class NIM(ModelInferenceTemplate):
    def __init__(
        self,
        node_selector: Optional[dict] = None,
        image: str = "nvcr.io/nim/meta/llama3-8b-instruct:1.0.0",
        health_endpoint: str = "v1/health/ready",
        port: int = 8000,
        cpu: int = 1,
        gpu: int = 1,
        mem: str = "20Gi",
        shm_size: str = "16Gi",
        # kubernetes secrets
        ngc_image_secret: Optional[str] = None,
        ngc_secret_group: Optional[str] = None,
        ngc_secret_key: Optional[str] = None,
        ####################
        env: Optional[dict[str, str]] = None,
        hf_repo_ids: Optional[list[str]] = None,
        hf_token_group: Optional[str] = None,
        hf_token_key: Optional[str] = None,
        lora_adapter_mem: Optional[str] = None,
    ):
        """
        Initialize NIM class for managing a Kubernetes pod template.

        :param node_selector: A dictionary representing the node selector for the Kubernetes pod.
        :param image: The Docker image to be used for the model server container. Default is "nvcr.io/nim/meta/llama3-8b-instruct:1.0.0".
        :param health_endpoint: The health endpoint for the model server container. Default is "v1/health/ready".
        :param port: The port number for the model server container. Default is 8000.
        :param cpu: The number of CPU cores requested for the model server container. Default is 1.
        :param gpu: The number of GPU cores requested for the model server container. Default is 1.
        :param mem: The amount of memory requested for the model server container. Default is "20Gi".
        :param shm_size: The size of the shared memory volume. Default is "16Gi".
        :param ngc_image_secret: The name of the Kubernetes secret containing the NGC image pull credentials.
        :param ngc_secret_group: The name of the Kubernetes secret group containing the NGC API key.
        :param ngc_secret_key: The key name for the NGC API key within the secret group.
        :param env: A dictionary of environment variables to be set in the model server container.
        :param hf_repo_ids: A list of Hugging Face repository IDs for LoRA adapters to be downloaded.
        :param hf_token_group: The name of the Kubernetes secret group containing the HuggingFace token.
        :param hf_token_key: The key name for the HuggingFace token within the secret group.
        :param lora_adapter_mem: The amount of memory requested for the init container that downloads LoRA adapters.
        """
        if ngc_image_secret is None:
            raise ValueError("NGC image pull credentials must be provided.")
        if ngc_secret_group is None:
            raise ValueError("NGC secret group must be provided.")
        if ngc_secret_key is None:
            raise ValueError("NGC secret key must be provided.")

        self._shm_size = shm_size
        self._ngc_image_secret = ngc_image_secret
        self._ngc_secret_group = ngc_secret_group
        self._ngc_secret_key = ngc_secret_key
        self._hf_repo_ids = hf_repo_ids
        self._hf_token_group = hf_token_group
        self._hf_token_key = hf_token_key
        self._lora_adapter_mem = lora_adapter_mem

        super().__init__(
            node_selector=node_selector,
            image=image,
            health_endpoint=health_endpoint,
            port=port,
            cpu=cpu,
            gpu=gpu,
            mem=mem,
            env=env,
        )

        self.nim_pod_template()

    def nim_pod_template(self):
        self.pod_template.pod_spec.volumes = [
            V1Volume(
                name="dshm",
                empty_dir=V1EmptyDirVolumeSource(medium="Memory", size_limit=self._shm_size),
            )
        ]
        self.pod_template.pod_spec.image_pull_secrets = [V1LocalObjectReference(name=self._ngc_image_secret)]

        model_server_container = self.pod_template.pod_spec.init_containers[0]
        model_server_container.env.append(
            V1EnvVar(
                name="NGC_API_KEY",
                value_from=V1EnvVarSource(
                    secret_key_ref=V1SecretKeySelector(
                        name=self._ngc_secret_group,
                        key=self._ngc_secret_key,
                    )
                ),
            )
        )
        model_server_container.volume_mounts = [V1VolumeMount(name="dshm", mount_path="/dev/shm")]
        model_server_container.security_context = V1SecurityContext(run_as_user=1000)

        # Download HF LoRA adapters
        if self._hf_repo_ids:
            if not self._lora_adapter_mem:
                raise ValueError("Memory to allocate to download LoRA adapters must be set.")

            local_peft_dir_env = next(
                (env for env in model_server_container.env if env.name == "NIM_PEFT_SOURCE"), None
            )
            if local_peft_dir_env:
                mount_path = local_peft_dir_env.value
            else:
                raise ValueError("NIM_PEFT_SOURCE must be set.")

            self.pod_template.pod_spec.volumes.append(V1Volume(name="lora", empty_dir={}))
            model_server_container.volume_mounts.append(V1VolumeMount(name="lora", mount_path=mount_path))

            self.pod_template.pod_spec.init_containers.insert(
                0,
                V1Container(
                    name="download-loras",
                    image="python:3.12-alpine",
                    command=[
                        "sh",
                        "-c",
                        f"""
            pip install -U "huggingface_hub[cli]"

            export LOCAL_PEFT_DIRECTORY={mount_path}
            mkdir -p $LOCAL_PEFT_DIRECTORY

            # If HF token is provided, log in
            if [ ! -z "$HF_TOKEN_GROUP" ] && [ ! -z "$HF_TOKEN_KEY" ]; then
                echo "$HF_TOKEN_GROUP:$HF_TOKEN_KEY" | huggingface-cli login --token
            fi

            # Download LoRAs from Huggingface Hub
            {"".join([f"""
            mkdir -p $LOCAL_PEFT_DIRECTORY/{repo_id.split("/")[-1]}
            huggingface-cli download {repo_id} adapter_config.json adapter_model.safetensors --local-dir $LOCAL_PEFT_DIRECTORY/{repo_id.split("/")[-1]}
            """ for repo_id in self._hf_repo_ids])}

            chmod -R 777 $LOCAL_PEFT_DIRECTORY
            """,
                    ],
                    resources=V1ResourceRequirements(
                        requests={"cpu": 1, "memory": self._lora_adapter_mem},
                        limits={"cpu": 1, "memory": self._lora_adapter_mem},
                    ),
                    volume_mounts=[
                        V1VolumeMount(
                            name="lora",
                            mount_path=mount_path,
                        )
                    ],
                ),
            )

            if self._hf_token_group and self._hf_token_key:
                self.pod_template.pod_spec.init_containers[0].env = [
                    V1EnvVar(name="HF_TOKEN_GROUP", value=self._hf_token_group),
                    V1EnvVar(name="HF_TOKEN_KEY", value=self._hf_token_key),
                ]
