from dataclasses import dataclass
from typing import Optional

from flyteidl.core.execution_pb2 import TaskExecution
from flytekitplugins.k8sdataservice.k8s.manager import K8sManager
from flytekitplugins.k8sdataservice.task import DataServiceConfig

from flytekit import logger
from flytekit.extend.backend.base_connector import AsyncConnectorBase, ConnectorRegistry, Resource, ResourceMeta
from flytekit.models.literals import LiteralMap
from flytekit.models.task import TaskTemplate


@dataclass
class DataServiceMetadata(ResourceMeta):
    dataservice_config: DataServiceConfig
    name: str


class DataServiceConnector(AsyncConnectorBase):
    name = "K8s DataService Async Connector"

    def __init__(self):
        self.k8s_manager = K8sManager()
        super().__init__(task_type_name="dataservicetask", metadata_type=DataServiceMetadata)
        self.config = None

    def create(
        self, task_template: TaskTemplate, output_prefix: str, inputs: Optional[LiteralMap] = None, **kwargs
    ) -> DataServiceMetadata:
        graph_engine_config = task_template.custom
        self.k8s_manager.set_configs(graph_engine_config)
        logger.info(f"Loaded connector config file {self.config}")
        existing_release_name = graph_engine_config.get("ExistingReleaseName", None)
        logger.info(f"The existing data service release name is {existing_release_name}")

        name = ""
        if existing_release_name is None or existing_release_name == "":
            logger.info("Creating K8s data service resources...")
            name = self.k8s_manager.create_data_service()
            logger.info(f'Data service {name} with image {graph_engine_config["Image"]} completed')
        else:
            name = existing_release_name
            logger.info(f"User configs to use the existing data service release name: {name}.")

        dataservice_config = DataServiceConfig(
            Name=graph_engine_config.get("Name", None),
            Image=graph_engine_config["Image"],
            Command=graph_engine_config["Command"],
            Cluster=graph_engine_config["Cluster"],
            ExistingReleaseName=graph_engine_config.get("ExistingReleaseName", None),
        )
        metadata = DataServiceMetadata(
            dataservice_config=dataservice_config,
            name=name,
        )
        logger.info(f"Created DataService metadata {metadata}")
        return metadata

    def get(self, resource_meta: DataServiceMetadata) -> Resource:
        logger.info("K8s Data Service get is called")
        data = resource_meta.dataservice_config
        data_dict = data.__dict__ if isinstance(data, DataServiceConfig) else data
        logger.info(f"The data_dict is {data_dict}")
        self.k8s_manager.set_configs(data_dict)
        name = data.Name
        logger.info(f"Get the stateful set name {name}")

        k8s_status = self.k8s_manager.check_stateful_set_status(name)
        flyte_state = None
        if k8s_status in ["failed", "timeout", "timedout", "canceled", "skipped", "internal_error"]:
            flyte_state = TaskExecution.FAILED
        elif k8s_status in ["done", "succeeded", "success"]:
            flyte_state = TaskExecution.SUCCEEDED
        elif k8s_status in ["running", "terminating", "pending"]:
            flyte_state = TaskExecution.RUNNING
        else:
            logger.error(f"Unrecognized state: {k8s_status}")
        outputs = {
            "data_service_name": name,
        }
        # TODO: Add logs for StatefulSet.
        return Resource(phase=flyte_state, outputs=outputs)

    def delete(self, resource_meta: DataServiceMetadata):
        logger.info("DataService delete is called")
        data = resource_meta.dataservice_config

        data_dict = data.__dict__ if isinstance(data, DataServiceConfig) else data
        self.k8s_manager.set_configs(data_dict)

        name = resource_meta.name
        logger.info(f"To delete the DataService (e.g., StatefulSet and Service) with name {name}")
        self.k8s_manager.delete_stateful_set(name)
        self.k8s_manager.delete_service(name)


ConnectorRegistry.register(DataServiceConnector())
