from typing import Dict, Optional, List, Type
import flytekit
from flytekit import FlyteContext, PythonFunctionTask, logger


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

class BaseCloudCredentialProvider:

    _CLOUD_TYPE: str = "base cloud",
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


