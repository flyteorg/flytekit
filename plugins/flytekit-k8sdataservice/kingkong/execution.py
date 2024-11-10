from typing import List, Optional, Dict
import requests
import json
from requests.packages.urllib3.exceptions import InsecureRequestWarning
from datetime import datetime
from flytekit import logger
from .utils import get_kingkong_endpoint
from utils.cluster_namespace import get_execution_namespace


class HelmExecutionContext:
    def __init__(self, type: str, config_name: str, installation_name: str):
        self.type = type
        self.config_name = config_name
        self.installation_name = installation_name


class CustomExecutionContext:
    pass


class Execution:
    def __init__(self, created_by: str, exec_type: str, namespace: str,
                 helm_context: Optional[HelmExecutionContext] = None,
                 custom_context: Optional[CustomExecutionContext] = None,
                 labels: Optional[List[str]] = None):
        self.created_by = created_by
        self.type = exec_type
        self.namespace = namespace
        self.helm_context = helm_context
        self.custom_context = custom_context
        self.labels = labels


class ResourceRequirements:
    def __init__(self, limits: Optional[Dict[str, str]] = None, requests: Optional[Dict[str, str]] = None):
        self.limits = limits
        self.requests = requests


class ContainerStateWaiting:
    def __init__(self, reason: str, message: Optional[str]):
        self.reason = reason
        self.message = message


class ContainerView:
    def __init__(self, image: Optional[str], name: str, restart_count: int,
                 resources: ResourceRequirements, state: Optional[dict]):
        self.image = image
        self.name = name
        self.restart_count = restart_count
        self.resources = resources
        self.state = state


class PodView:
    def __init__(self, last_transition_time: Optional[datetime], finished_at: Optional[datetime],
                 started_at: Optional[datetime], k8s_resource_id: Optional[str], node_name: Optional[str],
                 phase: Optional[str], containers: List[ContainerView]):
        self.last_transition_time = last_transition_time
        self.finished_at = finished_at
        self.started_at = started_at
        self.k8s_resource_id = k8s_resource_id
        self.node_name = node_name
        self.phase = phase
        self.containers = containers


class ResourceView:
    def __init__(self, pods: Optional[Dict[str, PodView]]):
        self.pods = pods


class ExecutionView:
    def __init__(self, execution: Execution, execution_id: Optional[int],
                 started_at: Optional[datetime], ended_at: Optional[datetime],
                 status: str, resources: Optional[ResourceView]):
        self.execution = execution
        self.id = execution_id
        self.started_at = started_at
        self.ended_at = ended_at
        self.status = status
        self.resources = resources


# Suppress only the single InsecureRequestWarning from urllib3
requests.packages.urllib3.disable_warnings(InsecureRequestWarning)


def create_kingkong_execution_id(cluster):

    # TODO: use the real ID when flyte SDK is upgraded security_context.get_run_as().get_execution_identity()
    id = 'default'

    namespace = get_execution_namespace(cluster)
    # Debugging info, simulate logging
    logger.info(f"Using kingkongConfig.Namespace: {namespace}")

    # Create the execution object
    execution = Execution(
        created_by=id,
        exec_type="Custom",
        namespace=namespace
    )

    # Convert the execution object to dictionary for JSON serialization
    exec_dict = {
        "createdBy": execution.created_by,
        "type": execution.type,
        "namespace": execution.namespace,
        "helmContext": None,
        "customContext": None,
    }

    req_body = json.dumps(exec_dict)

    try:
        response = requests.post(
            get_kingkong_endpoint(cluster=cluster),
            data=req_body,
            headers={"Content-Type": "application/json"},
            verify=False  # Ignore SSL for this example, change for production
        )

        if response.status_code not in [200, 201]:
            raise Exception(f"Request failed with status {response.status_code}")

        # Parse the response data
        data = response.json()

        # Deserialize the response into an ExecutionView object
        execution_view = ExecutionView(
            execution=execution,
            execution_id=data.get("id"),
            started_at=datetime.fromisoformat(data.get("startedAt")) if data.get("startedAt") else None,
            ended_at=datetime.fromisoformat(data.get("endedAt")) if data.get("endedAt") else None,
            status=data.get("status"),
            resources=None  # You can deserialize resources if needed
        )
    except Exception as e:
        logger.error(f"Error occurred in posting to King Kong: {str(e)}")
        return None
    return execution_view.id


def create_king_kong_execution_label(kk_execution_id: str):
    logger.info(f"the kingkong execution id is: {kk_execution_id}")
    label = {"kong.linkedin.com/executionID": str(kk_execution_id)}
    return label
