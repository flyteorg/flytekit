from asyncio.subprocess import PIPE
from decimal import ROUND_CEILING, Decimal
from typing import Optional, Tuple


from flyteidl.core.execution_pb2 import TaskExecution
from flytekit import FlyteContextManager
import flytekit
from flytekitplugins.skypilot.cloud_registry import BaseCloudCredentialProvider, \
    CloudRegistry, CloudCredentialError, CloudNotInstalledError
from flytekit.core.resources import Resources
from sky.skylet.job_lib import JobStatus
import subprocess

SKYPILOT_STATUS_TO_FLYTE_PHASE = {
    "INIT": TaskExecution.RUNNING,
    "PENDING": TaskExecution.RUNNING,
    "SETTING_UP": TaskExecution.RUNNING,
    "RUNNING": TaskExecution.RUNNING,
    "SUCCEEDED": TaskExecution.SUCCEEDED,
    "FAILED": TaskExecution.FAILED,
    "FAILED_SETUP": TaskExecution.FAILED,
    "CANCELLED": TaskExecution.FAILED,
}


def skypilot_status_to_flyte_phase(status: JobStatus) -> TaskExecution.Phase:
    """
    Map Skypilot status to Flyte phase.
    """
    return SKYPILOT_STATUS_TO_FLYTE_PHASE[status.value]



    
class AWSCredentialProvider(BaseCloudCredentialProvider):
    _CLOUD_TYPE: str = "aws"
    _SECRET_GROUP: Optional[str] = "aws-configure"

    def __init__(
        self,
    ):
        super().__init__()
    
    def check_cloud_dependency(self) -> None:
        try:
            version_check = subprocess.run(
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
            if configure_result.returncode!= 0:
                raise CloudCredentialError(f"Failed to configure AWS credentials for {key}: {configure_result.stderr.decode('utf-8')}")

CloudRegistry.register(AWSCredentialProvider._CLOUD_TYPE, AWSCredentialProvider)