import uuid

from flytekitplugins.k8sdataservice.k8s.kube_config import KubeConfig
from kubernetes import client
from kubernetes.client.rest import ApiException
from utils.resources import cleanup_resources, convert_flyte_to_k8s_fields

from flytekit import logger

APPNAME = "service-name"
DEFAULT_RESOURCES = client.V1ResourceRequirements(
    requests={"cpu": "2", "memory": "10G"}, limits={"cpu": "6", "memory": "16G"}
)


class K8sManager:
    def __init__(self):
        self.config = KubeConfig()
        self.config.load_kube_config()
        self.apps_v1_api = client.AppsV1Api()
        self.core_v1_api = client.CoreV1Api()

    def set_configs(self, data_service_config):
        self.data_service_config = data_service_config
        self.labels = {}
        self.namespace = "flyte"
        self.name = None
        self.name = data_service_config.get("Name", None)
        if self.name is None:
            self.name = f"k8s-dataservice-{uuid.uuid4().hex[:8]}"

    def create_data_service(self) -> str:
        svc_name = self.create_service()
        logger.info(f"Created service: {svc_name}")
        stateful_set_obj = self.create_stateful_set_object()
        name = self.create_stateful_set(stateful_set_obj)
        return name

    def create_stateful_set(self, stateful_set_object) -> str:
        api_response = None
        try:
            api_response = self.apps_v1_api.create_namespaced_stateful_set(
                namespace=self.namespace, body=stateful_set_object
            )
            logger.info(f"Created statefulset in K8s API server: {api_response}")
        except ApiException as e:
            logger.error(f"Exception when calling AppsV1Api->create_namespaced_stateful_set: {e}\n")
            raise
        return api_response.metadata.name

    def create_stateful_set_object(self):
        container = self._create_container()
        template = self._create_pod_template(container)
        spec = self._create_stateful_set_spec(template)
        return client.V1StatefulSet(
            api_version="apps/v1",
            kind="StatefulSet",
            metadata=client.V1ObjectMeta(
                labels=self.labels,
                name=self.name,
                annotations={},
            ),
            spec=spec,
        )

    def _create_container(self):
        ss_replicas = self.data_service_config.get("Replicas", 1)
        port = self.data_service_config.get("Port", 40000)
        ss_env = [
            client.V1EnvVar(name="GE_BASE_PORT", value=str(port)),
            client.V1EnvVar(name="GE_COUNT", value=str(int(ss_replicas))),
            client.V1EnvVar(name="SERVER_PORT", value=str(port)),
        ]
        return client.V1Container(
            name=self.name,
            image=self.data_service_config["Image"],
            image_pull_policy="IfNotPresent",
            ports=[client.V1ContainerPort(container_port=port, name="graph-engine")],
            command=self.data_service_config["Command"],
            env=ss_env,
            resources=self.get_resources(),
        )

    def _create_pod_template(self, container):
        self.labels.update({"app.kubernetes.io/instance": self.name})
        return client.V1PodTemplateSpec(
            metadata=client.V1ObjectMeta(
                labels=self.labels,
                annotations={},
            ),
            spec=client.V1PodSpec(
                containers=[container],
                security_context=client.V1PodSecurityContext(
                    fs_group=1001,
                    run_as_group=1001,
                    run_as_non_root=True,
                    run_as_user=1001,
                ),
            ),
        )

    def _create_stateful_set_spec(self, template):
        ss_replicas = self.data_service_config.get("Replicas", 1)
        return client.V1StatefulSetSpec(
            replicas=int(ss_replicas),
            selector=client.V1LabelSelector(
                match_labels={"app.kubernetes.io/instance": self.name},
            ),
            service_name=self.name,
            template=template,
        )

    def create_service(self) -> str:
        namespace = self.namespace
        logger.info(f"creating a service at namespace {namespace} with name {self.name}")
        port = self.data_service_config.get("Port", 40000)
        self.labels.update({"app.kubernetes.io/instance": self.name, "app": APPNAME})
        body = client.V1Service(
            api_version="v1",
            kind="Service",
            metadata=client.V1ObjectMeta(
                name=self.name,
                labels=self.labels,
                namespace=namespace,
            ),
            spec=client.V1ServiceSpec(
                selector={"app.kubernetes.io/instance": self.name},
                type="ClusterIP",
                ports=[
                    client.V1ServicePort(
                        port=port,
                        target_port=port,
                        name=self.name,
                    )
                ],
            ),
        )
        logger.info(
            f"Service configuration in namespace {namespace} and name {self.name} is completed, posting request to K8s API server..."
        )
        api_response = None
        try:
            api_response = self.core_v1_api.create_namespaced_service(namespace=namespace, body=body)
        except ApiException as e:
            logger.error(f"Exception when calling CoreV1Api->create_namespaced_service: {e}")
            raise e
        # This will not happen in K8s API, but in case.
        if api_response is None or not hasattr(api_response, "metadata") or not hasattr(api_response.metadata, "name"):
            raise ValueError("Invalid response from Kubernetes API - missing metadata or name")
        return api_response.metadata.name

    def check_stateful_set_status(self, name) -> str:
        try:
            stateful_set = self.apps_v1_api.read_namespaced_stateful_set(name=name, namespace=self.namespace)
            status = stateful_set.status
            logger.info(f"StatefulSet status: {status}")
            conditions = status.conditions if status and status.conditions else []
            logger.info(f"StatefulSet conditions: {conditions}")

            if status.replicas == 0:
                logger.info(
                    f"StatefulSet {name} is pending. replicas: {status.replicas}, available: {status.available_replicas }"
                )
                return "pending"

            if status.replicas > 0 and (
                status.replicas == status.available_replicas or status.replicas == status.ready_replicas
            ):
                logger.info(
                    f"StatefulSet {name} has succeeded. replicas: {status.replicas}, available: {status.available_replicas }"
                )
                return "success"

            if status.replicas > 0 and status.available_replicas is not None and status.available_replicas >= 0:
                logger.info(
                    f"StatefulSet {name} is running. replicas: {status.replicas}, available: {status.available_replicas }"
                )
                return "running"

            logger.info(
                f"StatefulSet {name} status is unknown. Replicas:  {status.replicas}, available: {status.available_replicas }"
            )
            return "failed"
        except ApiException as e:
            logger.error(f"Exception when calling AppsV1Api->read_namespaced_stateful_set: {e}")
            return f"Error checking status of StatefulSet {name}: {e}"

    def delete_stateful_set(self, name: str):
        try:
            self.apps_v1_api.delete_namespaced_stateful_set(
                name=name, namespace=self.namespace, body=client.V1DeleteOptions()
            )
            logger.info(f"Deleted StatefulSet: {name}")
        except ApiException as e:
            logger.error(f"Exception when calling AppsV1Api->delete_namespaced_stateful_set: {e}")

    def delete_service(self, name: str):
        try:
            self.core_v1_api.delete_namespaced_service(
                name=name, namespace=self.namespace, body=client.V1DeleteOptions()
            )
            logger.info(f"Deleted Service: {name}")
        except ApiException as e:
            logger.error(f"Exception when calling CoreV1Api->delete_namespaced_service: {e}")

    def get_resources(self) -> client.V1ResourceRequirements:
        res = DEFAULT_RESOURCES
        flyteidl_limits = self.data_service_config.get("Limits", None)
        flyteidl_requests = self.data_service_config.get("Requests", None)
        logger.info(f"Flyte Resources: limits: {flyteidl_limits} and requests {flyteidl_requests}")
        if flyteidl_limits is not None:
            res.limits = convert_flyte_to_k8s_fields(flyteidl_limits)
            logger.info(f"Resources limits updated is: {res.limits}")
        if flyteidl_requests is not None:
            res.requests = convert_flyte_to_k8s_fields(flyteidl_requests)
            logger.info(f"Resources requests updated is: {res.requests}")
        cleanup_resources(res)
        logger.info(f"Resources cleaned up is: {res}")
        return res
