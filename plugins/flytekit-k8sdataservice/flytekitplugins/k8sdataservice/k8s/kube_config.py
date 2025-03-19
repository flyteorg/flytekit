from kubernetes import config

from flytekit import logger


class KubeConfig:
    def __init__(self):
        pass

    def load_kube_config(self) -> None:
        """Load the kubernetes config based on fabric details prior to K8s client usage

        :params target_fabric: fabric on which we are loading configs
        """
        try:
            logger.info("Attempting to load in-cluster configuration.")
            config.load_incluster_config()  # This will use the service account credentials
            logger.info("Successfully loaded in-cluster configuration using the connector service account.")
        except config.ConfigException as e:
            logger.warning(f"Failed to load in-cluster configuration. {e}")
