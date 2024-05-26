import os
import subprocess
from asyncio.subprocess import PIPE
from dataclasses import dataclass
from typing import Dict, List, Optional, Type

import flytekit
from flytekit import logger


class CloudNotInstalledError(ValueError):
    """
    This is the base error for cloud credential errors.
    """

    pass


class CloudCredentialError(ValueError):
    """
    This is the base error for cloud credential errors.
    """

    pass


@dataclass
class CloudCredentialMount(object):
    vm_path: str
    container_path: str


class BaseCloudCredentialProvider:
    _CLOUD_TYPE: str = ("base cloud",)
    _SECRET_GROUP: Optional[str] = None

    def __init__(
        self,
    ):
        self._secret_manager = flytekit.current_context().secrets
        self.check_cloud_dependency()

    def check_cloud_dependency(self) -> None:
        raise NotImplementedError

    def setup_cloud_credential(
        self,
    ) -> None:
        raise NotImplementedError

    @property
    def secrets(self):
        return self._secret_manager

    @staticmethod
    def get_mount_envs() -> Dict[str, CloudCredentialMount]:
        return {}


class CloudRegistry(object):
    """
    This is the registry for all agents.
    The agent service will look up the agent registry based on the task type.
    The agent metadata service will look up the agent metadata based on the agent name.
    """

    _REGISTRY: Dict[str, Type[BaseCloudCredentialProvider]] = {}

    @staticmethod
    def register(cloud_type: str, provider: Type[BaseCloudCredentialProvider]):
        CloudRegistry._REGISTRY[cloud_type] = provider
        logger.info(f"Registering {cloud_type}")

    @staticmethod
    def get_cloud(task_type_name: str) -> Type[BaseCloudCredentialProvider]:
        # task_category = TaskCategory(name=task_type_name, version=task_type_version)
        if task_type_name not in CloudRegistry._REGISTRY:
            raise ValueError(f"Cannot find cloud for {task_type_name}")
        return CloudRegistry._REGISTRY[task_type_name]

    @staticmethod
    def list_clouds() -> List[Type[BaseCloudCredentialProvider]]:
        return list(CloudRegistry._REGISTRY.values())


class AWSCredentialProvider(BaseCloudCredentialProvider):
    _CLOUD_TYPE: str = "aws"
    _SECRET_GROUP: Optional[str] = "aws-configure"

    def __init__(
        self,
    ):
        super().__init__()

    def check_cloud_dependency(self) -> None:
        try:
            subprocess.run(
                [
                    "aws",
                    "--version",
                ],
                stdout=PIPE,
                stderr=PIPE,
            )
        except Exception as e:
            raise CloudNotInstalledError(
                f"AWS CLI not found. Please install it with 'pip install skypilot[aws]' and try again. Error: \n{type(e)}\n{e}"
            )

    def setup_cloud_credential(
        self,
    ) -> None:
        # self.check_cloud_dependency()
        secret_manager = self.secrets
        aws_config_dict = {
            "aws_access_key_id": secret_manager.get(
                group=self._SECRET_GROUP,
                key="aws_access_key_id",
            ),
            "aws_secret_access_key": secret_manager.get(
                group=self._SECRET_GROUP,
                key="aws_secret_access_key",
            ),
        }

        for key, secret in aws_config_dict.items():
            configure_result = subprocess.run(
                [
                    "aws",
                    "configure",
                    "set",
                    key,
                    secret,
                ],
                stdout=PIPE,
                stderr=PIPE,
            )
            if configure_result.returncode != 0:
                raise CloudCredentialError(
                    f"Failed to configure AWS credentials for {key}: {configure_result.stderr.decode('utf-8')}"
                )

    @staticmethod
    def get_mount_envs():
        return {
            "AWS_CONFIG_FILE": CloudCredentialMount(
                vm_path=("~/.aws/config"),
                container_path="/tmp/aws/config",
            ),
            "AWS_SHARED_CREDENTIALS_FILE": CloudCredentialMount(
                vm_path=("~/.aws/credentials"),
                container_path="/tmp/aws/credentials",
            ),
        }


class GCPCredentialProvider(BaseCloudCredentialProvider):
    _CLOUD_TYPE: str = "gcp"
    _SECRET_GROUP: Optional[str] = "gcloud"
    _GCLOUD_KEY_FILE: str = "/tmp/gcloud-flyte-key.json"

    def __init__(
        self,
    ):
        super().__init__()

    def check_cloud_dependency(self) -> None:
        try:
            subprocess.run(
                [
                    "gcloud",
                    "--version",
                ],
                stdout=PIPE,
                stderr=PIPE,
            )
        except Exception as e:
            raise CloudNotInstalledError(
                f"AWS CLI not found. Please install it with 'pip install skypilot[aws]' and try again. Error: \n{type(e)}\n{e}"
            )

    def setup_cloud_credential(
        self,
    ) -> None:
        # self.check_cloud_dependency()
        secret_manager = self.secrets
        gcp_config_dict = {
            "private_key": secret_manager.get(
                group=self._SECRET_GROUP,
                key="private_key",
            ).replace("\\n", "\n"),
            "client_email": secret_manager.get(
                group=self._SECRET_GROUP,
                key="client_email",
            ),
            "project_id": secret_manager.get(
                group=self._SECRET_GROUP,
                key="project_id",
            ),
        }
        gcp_config_dict.update(
            {
                "type": "service_account",
                "token_uri": "https://oauth2.googleapis.com/token",
            }
        )

        import json

        # FIXME: it looks insecure since the key file is on agent pod
        with open(self._GCLOUD_KEY_FILE, "w") as f:
            f.write(json.dumps(gcp_config_dict, indent=4))

        configure_result = subprocess.run(
            [
                "gcloud",
                "auth",
                "activate-service-account",
                "--key-file",
                self._GCLOUD_KEY_FILE,
            ],
            stdout=PIPE,
            stderr=PIPE,
        )
        if configure_result.returncode != 0:
            raise CloudCredentialError(
                f"Failed to configure GCP credentials: {configure_result.stderr.decode('utf-8')}"
            )

        os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = self._GCLOUD_KEY_FILE

    @staticmethod
    def get_mount_envs():
        return {
            "CLOUDSDK_CONFIG": CloudCredentialMount(
                vm_path=("~/.config/gcloud"),
                container_path="/tmp/gcloud",
            )
        }


CloudRegistry.register(AWSCredentialProvider._CLOUD_TYPE, AWSCredentialProvider)
CloudRegistry.register(GCPCredentialProvider._CLOUD_TYPE, GCPCredentialProvider)
