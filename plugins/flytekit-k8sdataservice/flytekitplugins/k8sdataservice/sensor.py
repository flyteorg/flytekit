from flytekitplugins.k8sdataservice.k8s.kube_config import KubeConfig
from kubernetes import client
from kubernetes.client.rest import ApiException

from flytekit import logger
from flytekit.sensor.base_sensor import BaseSensor

TRAININGJOB_API_GROUP = "kubeflow.org"
VERSION = "v1"


class CleanupSensor(BaseSensor):
    def __init__(self, name: str, **kwargs):
        """
        Initialize the CleanupSensor class with relevant configurations for monitoring and managing the k8s data service.
        """
        super().__init__(name=name, task_type="sensor", **kwargs)

    async def poke(self, release_name: str, cleanup_data_service: bool, cluster: str) -> bool:
        """poke will delete the graph engine resources based on the user's configuration
        1. This has to be done in the control plane by design. We don't expect any users's running pod to be authn/z to manage resources
        2. This can not be done in the async agent because the delete callback is only invoked on abortion operation or failure phase.
           while this makes sense but what we need is a separate task to delete graph engine without complicating the regular async agent flow.
        3. In the near future, we will add the poking logic on the training job's status. In the initial implementation, we skipped
        it for simplicity. This is also why we use the sensor API to keep forward compatibility
        """
        self.k8s_config = KubeConfig()
        self.k8s_config.load_kube_config()
        self.apps_v1_api = client.AppsV1Api()
        self.core_v1_api = client.CoreV1Api()
        self.custom_api = client.CustomObjectsApi()
        self.release_name = release_name
        self.cleanup_data_service = cleanup_data_service
        self.namespace = "flyte"
        self.cluster = cluster
        if not self.cleanup_data_service:
            logger.info(
                f"User decides to not to clean up the graph engine: {self.release_name} in cluster {self.cluster}, namespace {self.namespace}"
            )
            logger.info("DataService sensor will stop polling")
            return True
        # NOTE: the sensory node can be appended to the end of workflow.
        # So the training jobs are guaranteed to be finished, regardless of success or failure.
        logger.info(f"The training job is in terminal stage, deleting graph engine {self.release_name}")
        self.delete_data_service()
        return True

    def delete_data_service(self):
        """
        Delete the data service's associated Kubernetes resources (StatefulSet and Service).
        """
        data_service_name = self.release_name
        logger.info(f"Sensor got the release name: {self.release_name}")
        try:
            # Delete the Service associated with the graph engine
            self.core_v1_api.delete_namespaced_service(
                name=data_service_name, namespace=self.namespace, body=client.V1DeleteOptions()
            )
            logger.info(f"Deleted Service: {data_service_name}")
        except ApiException as e:
            logger.error(f"Error deleting Service: {e}")

        try:
            # Delete the StatefulSet associated with the graph engine
            self.apps_v1_api.delete_namespaced_stateful_set(
                name=data_service_name, namespace=self.namespace, body=client.V1DeleteOptions()
            )
            logger.info(f"Deleted StatefulSet: {data_service_name}")
        except ApiException as e:
            logger.error(f"Error deleting StatefulSet: {e}")
