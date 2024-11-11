from typing import Optional
from flytekit import logger
from kubernetes import client
from kubernetes.client.rest import ApiException
from flytekitplugins.k8sdataservice.k8s.kube_config import KubeConfig
# from kingkong.execution import create_king_kong_execution_label
from kingkong.utils import union_maps
from utils.cluster_namespace import get_execution_namespace
from utils.resources import cleanup_resources, convert_flyte_to_k8s_fields


# Default image if the user did not set it
# DEFAULT_IMAGE = "container-image-registry.corp.linkedin.com:8083/lps-image/linkedin/lpm-gnn/deepgraph-ge:3.0.82"
PORT = 40000
APPNAME = "service-name"
DEFAULT_RESOURCES = client.V1ResourceRequirements(
    requests={"cpu": "2", "memory": "10G"}, limits={"cpu": "6", "memory": "16G"})


class K8sManager:
    def __init__(self):
        self.config = KubeConfig()
        self.config.load_kube_config()
        self.apps_v1_api = client.AppsV1Api()
        self.core_v1_api = client.CoreV1Api()

    def set_configs(self, data_service_config):
        self.data_service_config = data_service_config
        self.labels = {}
        self.namespace = "flyte" # get_execution_namespace(data_service_config["Cluster"])
        self.name = None
        self.name = data_service_config.get("Name", None)
        if self.name is None:
            self.name = 'gnn-nofirsthash'

    def create_data_service(self, kk_execution_id: Optional[str] = None) -> str:
        # self.labels = create_king_kong_execution_label(kk_execution_id)
        svc_name = self.create_service()
        logger.info(f'Created service: {svc_name}')
        stateful_set_obj = self.create_stateful_set_object()
        name = self.create_stateful_set(stateful_set_obj)
        return name

    def create_stateful_set(self, stateful_set_object) -> str:
        api_response = None
        try:
            api_response = self.apps_v1_api.create_namespaced_stateful_set(
                namespace=self.namespace, body=stateful_set_object
            )
            logger.info(f'Created statefulset in K8s API server: {api_response}')
        except ApiException as e:
            logger.error(f"Exception when calling AppsV1Api->create_namespaced_stateful_set: {e}\n")
        if api_response is not None:
            return api_response.metadata.name
        return "failed_stateful_set_name"

    def create_stateful_set_object(self):
        ss_replicas = self.data_service_config.get("Replicas", 1)
        ss_env = [
            client.V1EnvVar(name="GE_BASE_PORT", value=str(PORT)),
            # I found that the the integer is serialized into float, and we have to
            # change it to int type first, and then to string type. Otherwise, it will be
            # something like "10.0".
            client.V1EnvVar(name="GE_COUNT", value=str(int(ss_replicas))),
            client.V1EnvVar(name="SERVER_PORT", value=str(PORT))
        ]
        container = client.V1Container(
            name=self.name,
            image=self.data_service_config["Image"],
            image_pull_policy="IfNotPresent",
            ports=[client.V1ContainerPort(container_port=PORT, name="graph-engine")],
            command=self.data_service_config["Command"],
            env=ss_env,
            resources=self.get_resources(),
            volume_mounts=[client.V1VolumeMount(
                mount_path="/etc/cm.json",
                name="cm-json-volume",
                read_only=True,
            )],
        )
        template = client.V1PodTemplateSpec(
            metadata=client.V1ObjectMeta(
                labels=union_maps(self.labels, {"app.kubernetes.io/instance": self.name}),
                annotations=self._get_annotations(),
            ),
            spec=client.V1PodSpec(
                containers=[container],
                volumes=[
                    client.V1Volume(
                        name="cm-json-volume",
                        host_path=client.V1HostPathVolumeSource(
                            path="/etc/cm.json",
                            type="FileOrCreate",
                        )
                    )],
                security_context=client.V1PodSecurityContext(
                    fs_group=1001,
                    run_as_group=1001,
                    run_as_non_root=True,
                    run_as_user=1001,
                ),

            )
        )
        spec = client.V1StatefulSetSpec(
            replicas=int(ss_replicas),
            selector=client.V1LabelSelector(
                match_labels={"app.kubernetes.io/instance": self.name},
            ),
            service_name=self.name,
            template=template)
        kingkong_label = union_maps(self.labels, {"kingkong.dl.linkedin.com/release-name": f'{self.name}'})
        statefulset = client.V1StatefulSet(
            api_version="apps/v1",
            kind="StatefulSet",
            metadata=client.V1ObjectMeta(
                labels=kingkong_label,
                name=self.name,
                annotations=self._get_annotations(),
            ),
            spec=spec)
        return statefulset

    def create_service(self) -> str:
        namespace = self.namespace
        logger.info(f"creating a service at namespace {namespace} with name {self.name}")
        label = union_maps(self.labels, {"app.kubernetes.io/instance": self.name, "app": APPNAME})
        body = client.V1Service(
            api_version="v1",
            kind="Service",
            metadata=client.V1ObjectMeta(
                name=self.name,
                labels=label,
                namespace=namespace,
            ),
            spec=client.V1ServiceSpec(
                selector={"app.kubernetes.io/instance": self.name},
                type="ClusterIP",
                ports=[client.V1ServicePort(
                    port=PORT,
                    target_port=PORT,
                    name=self.name,
                )],
            )
        )
        logger.info(f"Service configuration in namespace {namespace} and name {self.name} is completed, posting request to K8s API server...")
        api_response = None
        try:
            api_response = self.core_v1_api.create_namespaced_service(namespace=namespace, body=body)
        except Exception as e:
            logger.error(f"Exception when calling CoreV1Api->create_namespaced_service: {e}")
            raise e
        return api_response.metadata.name

    def check_stateful_set_status(self, name) -> str:
        try:
            stateful_set = self.apps_v1_api.read_namespaced_stateful_set(name=name, namespace=self.namespace)
            status = stateful_set.status
            logger.info(f"StatefulSet status: {status}")
            conditions = status.conditions if status and status.conditions else []
            logger.info(f"StatefulSet conditions: {conditions}")

            if status.replicas == 0:
                logger.info(f"StatefulSet {name} is pending. replicas: {status.replicas}, available: {status.available_replicas }")
                return "pending"

            if status.replicas > 0 and (status.replicas == status.available_replicas or status.replicas == status.ready_replicas):
                logger.info(f"StatefulSet {name} has succeeded. replicas: {status.replicas}, available: {status.available_replicas }")
                return "success"

            if status.replicas > 0 and status.available_replicas is not None and status.available_replicas >= 0:
                logger.info(f"StatefulSet {name} is running. replicas: {status.replicas}, available: {status.available_replicas }")
                return "running"

            logger.info(f"StatefulSet {name} status is unknown. Replicas:  {status.replicas}, available: {status.available_replicas }")
            return "failed"
        except ApiException as e:
            logger.error(f"Exception when calling AppsV1Api->read_namespaced_stateful_set: {e}")
            return f"Error checking status of StatefulSet {name}: {e}"

    def delete_stateful_set(self, name: str):
        try:
            self.apps_v1_api.delete_namespaced_stateful_set(name=name, namespace=self.namespace, body=client.V1DeleteOptions())
            logger.info(f'Deleted StatefulSet: {name}')
        except ApiException as e:
            logger.error(f"Exception when calling AppsV1Api->delete_namespaced_stateful_set: {e}")

    def delete_service(self, name: str):
        try:
            self.core_v1_api.delete_namespaced_service(name=name, namespace=self.namespace, body=client.V1DeleteOptions())
            logger.info(f'Deleted Service: {name}')
        except ApiException as e:
            logger.error(f"Exception when calling CoreV1Api->delete_namespaced_service: {e}")

    def _get_annotations(self):
        return {
            "kingkong.dl.linkedin.com/hdfs": f'{{"injectToken": true, "proxyAs": "{self.data_service_config["ProxyAs"]}"}}'
        }

    def get_execution_id_from_existing(self, release_name) -> str:
        # List the statefulset with the {"kingkong.dl.linkedin.com/release-name": name}
        try:
            api_response = self.apps_v1_api.list_namespaced_stateful_set(
                self.namespace,
                pretty='true',
                label_selector=f"kingkong.dl.linkedin.com/release-name={release_name}")
            statefulsets_list_size = len(api_response.items)
            kingkong_id_label_val = None
            if statefulsets_list_size != 0:
                logger.warning(f"Found {statefulsets_list_size} statefulset objects. Expected 1")
                # the hash name determines there will be only 1
                target_ss = api_response.items[0]
                target_ss_meta_labels = target_ss.metadata.labels
                logger.info(f"The listed statefulset labels are: {target_ss_meta_labels}")
                kingkong_id_label_val = target_ss_meta_labels.get("kong.linkedin.com/executionID")
                logger.info(f"The kingkong execution ID is {kingkong_id_label_val}")
                return kingkong_id_label_val
            else:
                logger.info("There is no existing data service")
        except ApiException as e:
            logger.error(f"Exception when calling AppsV1Api->list_namespaced_stateful_set: {e}")

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
