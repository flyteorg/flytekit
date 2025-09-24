import asyncio
import json
import re
import sys
import time
from dataclasses import dataclass
from typing import Any, Dict, Optional

from flyteidl.admin.agent_pb2 import GetTaskLogsResponse
from flyteidl.core.execution_pb2 import TaskExecution, TaskLog
from leptonai.api.v1.deployment import DeploymentAPI
from leptonai.api.v1.log import LogAPI
from leptonai.api.v1.types.common import Metadata
from leptonai.api.v1.types.deployment import LeptonDeployment, LeptonDeploymentUserSpec
from leptonai.api.v2.client import APIClient

from flytekit import current_context
from flytekit.extend.backend.base_connector import AsyncConnectorBase, ConnectorRegistry, Resource, ResourceMeta
from flytekit.extend.backend.utils import convert_to_flyte_phase
from flytekit.loggers import logger
from flytekit.models.literals import LiteralMap
from flytekit.models.task import TaskTemplate


def log_info(message: str):
    """Unified logging helper"""
    print(message)
    sys.stdout.flush()
    logger.info(message)


@dataclass
class LeptonMetadata(ResourceMeta):
    deployment_name: str


class LeptonEndpointConnector(AsyncConnectorBase):
    name = "Lepton Endpoint Connector"

    # Class constants for better performance
    ORIGIN_URL = "https://gateway.dgxc-lepton.nvidia.com"
    STATUS_MAPPING = {
        "starting": "pending",
        "scaling": "pending",
        "updating": "pending",
        "not ready": "pending",
        "notready": "pending",
        "ready": "succeeded",
        "deleting": "succeeded",
        "stopping": "succeeded",
        "terminating": "succeeded",
        "stopped": "succeeded",
        "terminated": "succeeded",
        "unknown": "failed",
        "unk": "failed",
    }

    # Pre-compiled regex for better performance
    NAME_SANITIZE_REGEX = re.compile(r"[^a-z0-9\-]")

    # Deployment type command generators (cached lambdas)
    DEPLOYMENT_COMMANDS = {
        "vllm": lambda config: [
            "vllm",
            "serve",
            config.get("checkpoint_path", "meta-llama/Llama-3.1-8B-Instruct"),
            f"--tensor-parallel-size={int(config.get('tensor_parallel_size', 1))}",
            f"--pipeline-parallel-size={int(config.get('pipeline_parallel_size', 1))}",
            f"--data-parallel-size={int(config.get('data_parallel_size', 1))}",
            f"--port={int(config.get('port', 8000))}",
            f"--served-model-name={config.get('served_model_name', 'default-model')}",
            *(config.get("extra_args", "").split() if config.get("extra_args") else []),
        ],
        "sglang": lambda config: [
            "python3",
            "-m",
            "sglang.launch_server",
            f"--model-path={config.get('checkpoint_path', 'meta-llama/Llama-3.1-8B-Instruct')}",
            "--host=0.0.0.0",
            f"--port={int(config.get('port', 8000))}",
            f"--served-model-name={config.get('served_model_name', 'default-model')}",
            f"--tp={int(config.get('tensor_parallel_size', 1))}",
            f"--dp={int(config.get('data_parallel_size', 1))}",
            *(config.get("extra_args", "").split() if config.get("extra_args") else []),
        ],
        "custom": lambda config: config.get(
            "command", ["/bin/bash", "-c", f"python3 -m http.server {int(config.get('port', 8080))} --bind 0.0.0.0"]
        ),
        "nim": lambda config: [],  # NIM uses default entrypoint
    }

    def __init__(self):
        super().__init__(task_type_name="lepton_endpoint_task", metadata_type=LeptonMetadata)
        # Single client with lazy-loaded API surfaces
        self._client: Optional[APIClient] = None
        self._deployment_api: Optional[DeploymentAPI] = None
        self._log_api: Optional[LogAPI] = None
        self._dashboard_base_url: Optional[str] = None

    def _sanitize_endpoint_name(self, name: str) -> str:
        """Optimized endpoint name sanitization"""
        # Use pre-compiled regex
        sanitized = self.NAME_SANITIZE_REGEX.sub("-", name.lower())

        # Ensure valid start/end characters
        if not sanitized[0].isalpha():
            sanitized = "ep-" + sanitized
        sanitized = sanitized.rstrip("-")
        if not sanitized[-1].isalnum():
            sanitized = sanitized + "x"

        # Efficient truncation with hash
        return sanitized[:33] + f"{hash(name) % 100:02d}" if len(sanitized) > 36 else sanitized

    def _get_dashboard_base_url(self) -> str:
        """Get dashboard base URL dynamically from workspace origin URL"""
        if self._dashboard_base_url is not None:
            return self._dashboard_base_url

        # Get origin URL from client
        try:
            origin_url = (
                self.client.workspace_origin_url if hasattr(self.client, "workspace_origin_url") else self.ORIGIN_URL
            )
        except Exception:
            origin_url = self.ORIGIN_URL

        # Convert gateway URL to dashboard URL
        if "gateway.dgxc-lepton.nvidia.com" in origin_url:
            self._dashboard_base_url = origin_url.replace(
                "gateway.dgxc-lepton.nvidia.com", "dashboard.dgxc-lepton.nvidia.com"
            )
        elif "gateway.lepton.ai" in origin_url:
            self._dashboard_base_url = origin_url.replace("gateway.lepton.ai", "dashboard.lepton.ai")
        else:
            # Fallback to hardcoded URL if pattern doesn't match
            self._dashboard_base_url = "https://dashboard.dgxc-lepton.nvidia.com"
            log_info(f"Warning: Using fallback dashboard URL for unknown origin: {origin_url}")

        log_info(f"Dynamic dashboard base URL: {self._dashboard_base_url}")
        return self._dashboard_base_url

    def _build_logs_url(self, deployment_name: str, workspace_id: str) -> str:
        """Build dynamic logs URL for deployment"""
        dashboard_base = self._get_dashboard_base_url()
        return f"{dashboard_base}/workspace/{workspace_id}/compute/deployments/detail/{deployment_name}/replicas/list#/deployment/{deployment_name}/logs"

    @property
    def client(self) -> APIClient:
        """Lazy-loaded, cached API client"""
        if self._client is not None:
            return self._client

        import os

        workspace_id = os.environ.get("LEPTON_WORKSPACE_ID")
        token = os.environ.get("LEPTON_TOKEN")

        # Fallback to Flyte secrets
        if not workspace_id or not token:
            try:
                secrets = current_context().secrets
                workspace_id = workspace_id or secrets.get("lepton_workspace_id")
                token = token or secrets.get("lepton_token")
            except Exception:
                pass

        if not workspace_id or not token:
            raise ValueError("Missing Lepton credentials: LEPTON_WORKSPACE_ID and LEPTON_TOKEN")

        log_info(f"Creating Lepton API client for workspace: {workspace_id}")
        self._client = APIClient(workspace_id=workspace_id, auth_token=token, workspace_origin_url=self.ORIGIN_URL)
        return self._client

    @property
    def deployment_api(self) -> DeploymentAPI:
        """Lazy-loaded deployment API"""
        if self._deployment_api is None:
            self._deployment_api = DeploymentAPI(self.client)
        return self._deployment_api

    @property
    def log_api(self) -> LogAPI:
        """Lazy-loaded log API"""
        if self._log_api is None:
            self._log_api = LogAPI(self.client)
        return self._log_api

    def _map_status_to_phase(self, status: str) -> TaskExecution.Phase:
        """Optimized status mapping with fallback"""
        return convert_to_flyte_phase(self.STATUS_MAPPING.get(status.lower() if status else "unknown", "pending"))

    def _build_spec(self, config: Dict[str, Any]) -> Dict[str, Any]:
        """Streamlined spec generation"""
        deployment_type = config.get("deployment_type", "custom")
        port = int(config.get("port", 8080 if deployment_type == "custom" else 8000))

        # Base container spec
        container = {"image": config.get("image", "python:3.11-slim"), "ports": [{"container_port": port}]}

        # Add command if needed
        if deployment_type in self.DEPLOYMENT_COMMANDS:
            command = self.DEPLOYMENT_COMMANDS[deployment_type](config)
            if command:
                container["command"] = command

        # Build environment variables efficiently
        envs = []

        # Auto-derived envs
        auto_envs = [
            ("SERVED_MODEL_NAME", config.get("served_model_name")),
            ("MODEL_PORT", str(port) if config.get("port") else None),
            ("MODEL_PATH", config.get("checkpoint_path") if deployment_type in ["vllm", "sglang"] else None),
            (
                "TENSOR_PARALLEL_SIZE",
                str(int(config.get("tensor_parallel_size", 1))) if deployment_type in ["vllm", "sglang"] else None,
            ),
            ("NIM_MODEL_NAME", config.get("served_model_name") if deployment_type == "nim" else None),
        ]

        envs.extend([{"name": k, "value": v} for k, v in auto_envs if v])

        # User-defined envs
        for key, value in config.get("env_vars", {}).items():
            env_var = {"name": key}
            if isinstance(value, dict) and "value_from" in value:
                env_var["value_from"] = value["value_from"]
            else:
                env_var["value"] = str(value)
            envs.append(env_var)

        # Build API tokens
        api_tokens = []
        for token_config in config.get("api_tokens", []):
            if isinstance(token_config, dict):
                if "value" in token_config:
                    api_tokens.append({"value": str(token_config["value"])})
                elif "value_from" in token_config:
                    api_tokens.append({"value_from": token_config["value_from"]})
            else:
                api_tokens.append({"value": str(token_config)})

        # Legacy support
        if not api_tokens and config.get("api_token"):
            api_tokens.append({"value": config["api_token"]})

        # Assemble final spec
        spec = {
            "container": container,
            "resource_requirement": {
                "resource_shape": config.get("resource_shape", "cpu.small"),
                "min_replicas": config.get("min_replicas", 1),
                "max_replicas": config.get("max_replicas", 1),
            },
            "envs": envs,
            "health": config.get("health_config", {}),
            "log": {"enable_collection": config.get("log_config", {}).get("enable_collection", True)},
            "metrics": {},
            "routing_policy": {},
            "enable_rdma": config.get("enable_rdma", False),
            "user_security_context": {},
        }

        # Add optional configurations efficiently
        optional_fields = [
            ("auto_scaler", config.get("auto_scaler")),
            ("queue_config", config.get("queue_config")),
            ("image_pull_secrets", config.get("image_pull_secrets")),
            ("api_tokens", api_tokens if api_tokens else None),
        ]

        for field_name, value in optional_fields:
            if value:
                spec[field_name] = value

        # Node group affinity
        if config.get("node_group"):
            spec["resource_requirement"]["affinity"] = {"allowed_dedicated_node_groups": [config["node_group"]]}

        # Storage mounts
        mounts = config.get("mounts", {})
        if mounts.get("enabled"):
            spec["mounts"] = [
                {
                    "path": mounts["cache_path"],
                    "from": mounts.get("storage_source", "node-nfs:lepton-shared-fs"),
                    "mount_path": mounts["mount_path"],
                    "mount_options": {},
                }
            ]

        return spec

    def _extract_config_from_inputs(self, inputs: Optional[LiteralMap]) -> Dict[str, Any]:
        """Optimized config extraction"""
        if not inputs or not inputs.literals:
            return {}

        config = {}
        for key, literal in inputs.literals.items():
            if not literal.scalar:
                continue

            if literal.scalar.primitive:
                primitive = literal.scalar.primitive
                # Use getattr for cleaner code
                for attr, default in [
                    ("integer", None),
                    ("float_value", None),
                    ("string_value", None),
                    ("boolean", None),
                ]:
                    value = getattr(primitive, attr, default)
                    if value is not None:
                        config[key] = value
                        break
            elif literal.scalar.generic:
                try:
                    config[key] = json.loads(literal.scalar.generic.value.decode("utf-8"))
                except (json.JSONDecodeError, ValueError):
                    config[key] = literal.scalar.generic.value.decode("utf-8")

        log_info(f"Extracted config: {list(config.keys())}")
        return config

    async def create(
        self, task_template: TaskTemplate, inputs: Optional[LiteralMap] = None, **kwargs
    ) -> LeptonMetadata:
        """Optimized endpoint creation"""
        log_info(f"Processing task: {task_template.id}")

        # Efficient config merging
        task_config = task_template.custom if isinstance(task_template.custom, dict) else {}
        input_config = self._extract_config_from_inputs(inputs)
        config = {**task_config, **input_config}

        log_info(f"Config keys: {list(config.keys())}")

        # Generate name and spec
        endpoint_name = config.get("endpoint_name") or f"{task_template.id.name}-{int(time.time())}"
        endpoint_name = self._sanitize_endpoint_name(endpoint_name)

        log_info(f"Creating: {endpoint_name}")

        try:
            spec = self._build_spec(config)

            # Create deployment object
            metadata = Metadata(id_=endpoint_name, name=endpoint_name)
            user_spec = LeptonDeploymentUserSpec(**spec)
            deployment_obj = LeptonDeployment(metadata=metadata, spec=user_spec)

            # Execute in thread pool (SDK is sync)
            await asyncio.get_event_loop().run_in_executor(None, lambda: self.deployment_api.create(deployment_obj))

            log_info(f"Created: {endpoint_name}")
            return LeptonMetadata(deployment_name=endpoint_name)

        except Exception as e:
            log_info(f"Failed: {e}")
            raise RuntimeError(f"Failed to create Lepton endpoint: {e}")

    async def get(self, resource_meta: LeptonMetadata, **kwargs) -> Resource:
        """Optimized status checking"""
        deployment_name = resource_meta.deployment_name
        log_info(f"Checking: {deployment_name}")

        try:
            deployment = await asyncio.get_event_loop().run_in_executor(
                None, lambda: self.deployment_api.get(deployment_name)
            )

            if not deployment:
                return Resource(phase=convert_to_flyte_phase("failed"))

            # Extract state efficiently
            state = "unknown"
            if deployment.status and deployment.status.state:
                state_str = str(deployment.status.state)
                state = state_str.split(".")[-1].lower() if "LeptonDeploymentState." in state_str else state_str.lower()

            log_info(f"State: {state}")
            flyte_phase = self._map_status_to_phase(state)

            # Build dynamic log links
            workspace_id = self.client.workspace_id
            logs_url = self._build_logs_url(deployment_name, workspace_id)
            log_links = [TaskLog(uri=logs_url, name="Lepton Console")]

            # Return appropriate response
            if flyte_phase == TaskExecution.SUCCEEDED:
                # Get endpoint URL
                external_endpoint = "Unknown URL"
                if deployment.status and deployment.status.endpoint and deployment.status.endpoint.external_endpoint:
                    external_endpoint = deployment.status.endpoint.external_endpoint

                log_info(f"Ready: {external_endpoint}")
                return Resource(
                    phase=flyte_phase,
                    message=f"Status: {state}",
                    log_links=log_links,
                    outputs={"o0": external_endpoint},
                )
            else:
                log_info(f"Not ready: {state}")
                return Resource(
                    phase=flyte_phase,
                    message=f"Status: {state}",
                    log_links=log_links,
                )

        except Exception as e:
            log_info(f"Error: {e}")
            return Resource(phase=convert_to_flyte_phase("failed"))

    async def delete(self, resource_meta: LeptonMetadata, **kwargs):
        """Optimized deletion with idempotency"""
        deployment_name = resource_meta.deployment_name

        try:
            log_info(f"Deleting: {deployment_name}")

            await asyncio.get_event_loop().run_in_executor(None, lambda: self.deployment_api.delete(deployment_name))

            log_info(f"Deleted: {deployment_name}")

        except Exception as e:
            error_msg = str(e).lower()
            # Idempotent deletion
            if any(term in error_msg for term in ["not found", "does not exist", "404"]):
                log_info(f"Already deleted: {deployment_name}")
            else:
                logger.error(f"Delete failed: {e}")
                raise RuntimeError(f"Failed to delete: {e}")

    def get_logs(self, resource_meta: LeptonMetadata, **kwargs) -> GetTaskLogsResponse:
        """Optimized log retrieval"""
        deployment_name = resource_meta.deployment_name

        try:
            log_info(f"Getting logs: {deployment_name}")

            logs_result = self.log_api.get_log(deployment=deployment_name, num=1000)
            log_lines = logs_result.split("\n") if isinstance(logs_result, str) else []

            # Build consistent console URL using same logic as get() method
            workspace_id = self.client.workspace_id
            console_url = self._build_logs_url(deployment_name, workspace_id)

            return GetTaskLogsResponse(
                logs=log_lines, token="", log_links=[TaskLog(name="Lepton Console", uri=console_url)]
            )

        except Exception as e:
            logger.error(f"Log retrieval failed: {e}")
            return GetTaskLogsResponse(logs=[], token="", log_links=[])


# Register the connector
ConnectorRegistry.register(LeptonEndpointConnector())

# Backward compatibility alias
LeptonConnector = LeptonEndpointConnector
