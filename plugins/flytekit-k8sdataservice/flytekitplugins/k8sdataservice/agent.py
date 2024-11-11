from dataclasses import asdict, dataclass
from typing import Optional
import datetime
import json
import grpc
# import yaml
# from flytekit.models.core.execution import TaskLog
# from flytekit.models import literals
from flytekitplugins.k8sdataservice.task import DataServiceConfig
from flytekitplugins.k8sdataservice.k8s.manager import K8sManager
from flytekit import FlyteContextManager, logger
from flytekit.core.type_engine import TypeEngine
from flyteidl.core.execution_pb2 import TaskExecution
from flytekit.extend.backend.base_agent import AsyncAgentBase, AgentRegistry, Resource, ResourceMeta
from flytekit.models.literals import LiteralMap
from flytekit.models.task import TaskTemplate
# from kingkong.execution import create_kingkong_execution_id


@dataclass
class DataServiceMetadata(ResourceMeta):
    dataservice_config: DataServiceConfig
    name: str


class DataServiceAgent(AsyncAgentBase):
    name = "K8s DataService Async Agent"
    # config_file_path = "/etc/config/aipflyteagent/task_logs.yaml"

    def __init__(self):
        self.k8s_manager = K8sManager()
        super().__init__(task_type_name="dataservicetask", metadata_type=DataServiceMetadata)
        self.config = None
        self.kk_execution_id = None

    def create(
        self,
        task_template: TaskTemplate,
        output_prefix: str,
        inputs: Optional[LiteralMap] = None,
         **kwargs
    ) -> DataServiceMetadata:
        graph_engine_config = task_template.custom
        self.k8s_manager.set_configs(graph_engine_config)
        # with open(DataServiceAgent.config_file_path, 'r') as file:
        #     self.config = yaml.safe_load(file)
        logger.info(f"Loaded agent config file {self.config}")
        existing_release_name = graph_engine_config.get("ExistingReleaseName", None)
        logger.info(f"The existing data service release name is {existing_release_name}")

        name = ""
        if existing_release_name is None or existing_release_name == "":
            logger.info('Creating K8s data service resources...')
            # self.kk_execution_id = create_kingkong_execution_id(cluster=graph_engine_config["Cluster"])
            # logger.info(f'The created KingKong execution ID: {self.kk_execution_id}.')
            name = self.k8s_manager.create_data_service(self.kk_execution_id)
            logger.info(f'Data service {name} with image {graph_engine_config["Image"]} completed')
        else:
            name = existing_release_name
            logger.info(f'User configs to use the existing data service release name: {name}.')
            # self.kk_execution_id = self.k8s_manager.get_execution_id_from_existing(release_name=existing_release_name)
            logger.info(f'The existing execution ID found is: {self.kk_execution_id}.')

        dataservice_config = DataServiceConfig(
            Name=graph_engine_config.get("Name", None),
            Image=graph_engine_config["Image"],
            Command=graph_engine_config["Command"],
            ProxyAs=graph_engine_config.get("ProxyAs", None),
            Cluster=graph_engine_config["Cluster"],
            ExistingReleaseName=graph_engine_config.get("ExistingReleaseName", None)
        )
        metadata = DataServiceMetadata(
            dataservice_config=dataservice_config,
            name=name,
        )
        logger.info(f'Created DataService metadata {metadata}')
        return metadata

    def get(self, resource_meta: DataServiceMetadata) -> Resource:
        logger.info("K8s Data Service get is called")
        data = resource_meta.dataservice_config
        data_dict = data.__dict__ if isinstance(data, DataServiceConfig) else data
        logger.info(f"The data_dict is {data_dict}")
        self.k8s_manager.set_configs(data_dict)
        name = data.Name
        logger.info(f'Get the stateful set name {name}')

        ctx = FlyteContextManager.current_context()
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
        # template logs
        # template_uri = (
        #     self.config.get('task_logs', {})
        #     .get('templates', [{}])[0]
        #     .get('templateUris', [None])[0]
        # )
        # if template_uri and self.kk_execution_id:
        #     actual_log_url = template_uri.replace("{{ .kingKongID }}", str(self.kk_execution_id))
        # else:
        #     actual_log_url = None
        # if actual_log_url is None:
        #     logger.warning("Template URI or KingKongID not found, unable to construct log URL.")
        # display_name = (
        #     self.config.get('task_logs', {})
        #     .get('templates', [{}])[0]
        #     .get('displayName', 'default_display_name')
        # )
        # log_links = [
        #     TaskLog(uri="placeholder", name="Service log",
        #             message_format=TaskLog.MessageFormat.JSON, ttl=datetime.timedelta(days=90)).to_flyte_idl()
        # ]
        # logger.info(f"the log_links are {log_links}")
        return Resource(phase=flyte_state, outputs=outputs)

    def delete(self, resource_meta: DataServiceMetadata):
        logger.info("DataService delete is called")
        data = resource_meta.dataservice_config

        data_dict = data.__dict__ if isinstance(data, DataServiceConfig) else data
        self.k8s_manager.set_configs(data_dict)
        
        name = resource_meta.name
        logger.info(f'To delete the DataService (e.g., StatefulSet and Service) with name {name}')
        self.k8s_manager.delete_stateful_set(name)
        self.k8s_manager.delete_service(name)


AgentRegistry.register(DataServiceAgent())
# The sensor task type with the Sensor, it is always in the flytekit that LinkedIn don't need.
# Data triggered sensor owned by the flyte team is directly using the default sensor task type
# name and Sensor Metadata, which can cause the request can send to other agents as all the agents
# need to import the flytekit.
# here we force to remove the sensor to avoid the mess up completely.
# The better approach is to chase the flyte team to keep the flyte backend and frontend upgrade
# as soon as possible!
# del AgentRegistry._REGISTRY["sensor"]
# del AgentRegistry._METADATA["Sensor"]
