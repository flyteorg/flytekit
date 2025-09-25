"""Optimized Lepton AI connector with improved performance and reduced redundancy."""

import asyncio
import re
import time
from dataclasses import dataclass
from functools import lru_cache
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


def log_info(message: str) -> None:
    """Optimized unified logging helper."""
    logger.info(message)
    print(message, flush=True)  # More efficient than separate flush call


@dataclass
class LeptonMetadata(ResourceMeta):
    deployment_name: str


class BaseLeptonConnector(AsyncConnectorBase):
    """Base class for Lepton connectors with shared functionality."""

    # Optimized class constants
    ORIGIN_URL = "https://gateway.dgxc-lepton.nvidia.com"

    # Consolidated status mapping with frozenset for faster lookup
    _PENDING_STATES = frozenset(["starting", "scaling", "updating", "not ready", "notready"])
    _SUCCESS_STATES = frozenset(["ready", "deleting", "stopping", "terminating", "stopped", "terminated"])
    _FAILED_STATES = frozenset(["unknown", "unk"])

    # Pre-compiled regex for better performance
    NAME_SANITIZE_REGEX = re.compile(r"[^a-z0-9\-]")

    @staticmethod
    def _get_deployment_command(deployment_type: str, config: Dict[str, Any]) -> list:
        """Generate deployment command based on type and config."""
        if deployment_type == "vllm":
            return [
                "vllm",
                "serve",
                config.get("checkpoint_path", "meta-llama/Llama-3.1-8B-Instruct"),
                f"--tensor-parallel-size={int(config.get('tensor_parallel_size', 1))}",
                f"--pipeline-parallel-size={int(config.get('pipeline_parallel_size', 1))}",
                f"--data-parallel-size={int(config.get('data_parallel_size', 1))}",
                f"--port={int(config.get('port', 8000))}",
                f"--served-model-name={config.get('served_model_name', 'default-model')}",
            ] + (config.get("extra_args", "").split() if config.get("extra_args") else [])
        elif deployment_type == "sglang":
            return [
                "python3",
                "-m",
                "sglang.launch_server",
                f"--model-path={config.get('checkpoint_path', 'meta-llama/Llama-3.1-8B-Instruct')}",
                "--host=0.0.0.0",
                f"--port={int(config.get('port', 8000))}",
                f"--served-model-name={config.get('served_model_name', 'default-model')}",
                f"--tp={int(config.get('tensor_parallel_size', 1))}",
                f"--dp={int(config.get('data_parallel_size', 1))}",
            ] + (config.get("extra_args", "").split() if config.get("extra_args") else [])
        elif deployment_type == "custom":
            return config.get(
                "command", ["/bin/bash", "-c", f"python3 -m http.server {int(config.get('port', 8080))} --bind 0.0.0.0"]
            )
        else:  # nim or unknown
            return []

    def __init__(self, task_type_name: str, connector_name: str):
        super().__init__(task_type_name=task_type_name, metadata_type=LeptonMetadata)
        self.name = connector_name
        # Single client with lazy-loaded API surfaces
        self._client: Optional[APIClient] = None
        self._deployment_api: Optional[DeploymentAPI] = None
        self._log_api: Optional[LogAPI] = None
        self._dashboard_base_url: Optional[str] = None

    @lru_cache(maxsize=128)
    def _sanitize_endpoint_name(self, name: str) -> str:
        """Optimized endpoint name sanitization with caching."""
        sanitized = self.NAME_SANITIZE_REGEX.sub("-", name.lower())

        # Ensure valid start/end characters
        if not sanitized or not sanitized[0].isalpha():
            sanitized = "ep-" + sanitized
        sanitized = sanitized.rstrip("-")
        if not sanitized or not sanitized[-1].isalnum():
            sanitized = sanitized + "x"

        # Efficient truncation with deterministic suffix
        if len(sanitized) > 36:
            return sanitized[:33] + f"{hash(name) % 1000:03d}"
        return sanitized

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
        """Optimized status mapping using frozensets for faster lookup."""
        if not status:
            return convert_to_flyte_phase("pending")

        status_lower = status.lower()
        if status_lower in self._PENDING_STATES:
            return convert_to_flyte_phase("pending")
        elif status_lower in self._SUCCESS_STATES:
            return convert_to_flyte_phase("succeeded")
        elif status_lower in self._FAILED_STATES:
            return convert_to_flyte_phase("failed")
        else:
            return convert_to_flyte_phase("pending")  # Default fallback

    def _build_spec(self, config: Dict[str, Any]) -> Dict[str, Any]:
        """Optimized spec generation with reduced complexity."""
        deployment_type = config.get("deployment_type", "custom")
        port = config.get("port", 8080 if deployment_type == "custom" else 8000)

        log_info(f"Building spec for {deployment_type}, port: {port}")

        # Base container spec
        container = {"image": config.get("image", "python:3.11-slim"), "ports": [{"container_port": port}]}

        # Add command using optimized method
        command = self._get_deployment_command(deployment_type, config)
        if command:
            container["command"] = command
            log_info(f"Generated command: {' '.join(command[:3])}...")

        # Build environment variables efficiently
        envs = []

        # Auto-derived environment variables
        if config.get("served_model_name"):
            envs.append({"name": "SERVED_MODEL_NAME", "value": config["served_model_name"]})
        if config.get("port"):
            envs.append({"name": "MODEL_PORT", "value": str(port)})
        if deployment_type in ("vllm", "sglang") and config.get("checkpoint_path"):
            envs.append({"name": "MODEL_PATH", "value": config["checkpoint_path"]})
        if deployment_type in ("vllm", "sglang"):
            envs.append({"name": "TENSOR_PARALLEL_SIZE", "value": str(config.get("tensor_parallel_size", 1))})
        if deployment_type == "nim" and config.get("served_model_name"):
            envs.append({"name": "NIM_MODEL_NAME", "value": config["served_model_name"]})

        # User-defined environment variables
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

        # Assemble final spec more efficiently
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

        # Add optional configurations
        if config.get("auto_scaler"):
            spec["auto_scaler"] = config["auto_scaler"]
        if config.get("queue_config"):
            spec["queue_config"] = config["queue_config"]
        if config.get("image_pull_secrets"):
            spec["image_pull_secrets"] = config["image_pull_secrets"]
        if api_tokens:
            spec["api_tokens"] = api_tokens

        # Node group affinity
        if config.get("node_group"):
            spec["resource_requirement"]["affinity"] = {"allowed_dedicated_node_groups": [config["node_group"]]}

        # Storage mounts - simplified handling
        mounts_config = config.get("mounts")
        if mounts_config:
            mounts_list = (
                mounts_config.get("mounts")
                if isinstance(mounts_config, dict) and "mounts" in mounts_config
                else mounts_config
                if isinstance(mounts_config, list)
                else []
            )

            if mounts_list:
                spec["mounts"] = [
                    {
                        "path": mount["cache_path"],
                        "from": mount.get("storage_source", "node-nfs:lepton-shared-fs"),
                        "mount_path": mount["mount_path"],
                        "mount_options": mount.get("mount_options", {}),
                    }
                    for mount in mounts_list
                    if isinstance(mount, dict) and mount.get("enabled", True)
                ]

        return spec

    def _extract_config_from_inputs(self, task_template: TaskTemplate, inputs: Optional[LiteralMap]) -> Dict[str, Any]:
        """Extract config from inputs using Flytekit's proper TypeEngine approach"""
        if not inputs or not inputs.literals:
            return {}

        try:
            # Use the proper Flytekit pattern from Perian and Snowflake connectors
            from flytekit import FlyteContextManager
            from flytekit.core.type_engine import TypeEngine

            ctx = FlyteContextManager.current_context()
            literal_types = task_template.interface.inputs

            # Convert literals to native Python objects using TypeEngine
            native_inputs = TypeEngine.literal_map_to_kwargs(ctx, inputs, literal_types=literal_types)
            log_info(f"Native inputs from TypeEngine: {list(native_inputs.keys())}")

            config = {}

            # Handle the request dataclass specially
            if "request" in native_inputs:
                request_data = native_inputs["request"]
                log_info(f"Found request data: {type(request_data)}")

                # If it's a dataclass instance, extract its fields
                if hasattr(request_data, "__dataclass_fields__"):
                    log_info(f"Request is a dataclass with fields: {list(request_data.__dataclass_fields__.keys())}")
                    # Extract all fields from the dataclass instance
                    for field_name in request_data.__dataclass_fields__.keys():
                        value = getattr(request_data, field_name)
                        if value is not None:
                            config[field_name] = value
                            log_info(f"Extracted {field_name} from dataclass: {value}")

                    log_info(
                        f"Successfully extracted {len([v for v in config.values() if v is not None])} parameters from dataclass"
                    )
                elif isinstance(request_data, dict):
                    log_info(f"Request is a dict with keys: {list(request_data.keys())}")
                    # Extract all fields from the dict
                    for param, value in request_data.items():
                        if value is not None:
                            config[param] = value
                            log_info(f"Extracted {param} from dict: {value}")
                else:
                    # Store as-is if it's neither a dataclass nor dict
                    config["request"] = request_data
                    log_info(f"Stored request as-is: {type(request_data)}")

            # Add any other top-level inputs
            for key, value in native_inputs.items():
                if key != "request" and value is not None:
                    config[key] = value
                    log_info(f"Added top-level input {key}: {value}")

            log_info(f"Final config keys: {list(config.keys())}")
            return config

        except Exception as e:
            log_info(f"Error extracting config from inputs: {e}")
            import traceback

            log_info(f"Traceback: {traceback.format_exc()}")
            # Fallback to empty config
            return {}

    async def create(
        self, task_template: TaskTemplate, inputs: Optional[LiteralMap] = None, **kwargs
    ) -> LeptonMetadata:
        """Handle endpoint operations - to be overridden by subclasses"""
        log_info(f"Processing task: {task_template.id}")

        # Efficient config merging
        task_config = task_template.custom if isinstance(task_template.custom, dict) else {}
        input_config = self._extract_config_from_inputs(task_template, inputs)
        config = {**task_config, **input_config}

        log_info(f"Config keys: {list(config.keys())}")

        # Delegate to specific implementation
        return await self._handle_operation(config, task_template)

    async def _handle_operation(self, config: Dict[str, Any], task_template: TaskTemplate) -> LeptonMetadata:
        """To be implemented by subclasses"""
        raise NotImplementedError("Subclasses must implement _handle_operation")

    async def _handle_deployment(self, config: Dict[str, Any], task_template: TaskTemplate) -> LeptonMetadata:
        """Handle endpoint deployment"""
        # Generate name and spec
        endpoint_name = config.get("endpoint_name") or f"{task_template.id.name}-{int(time.time())}"
        endpoint_name = self._sanitize_endpoint_name(endpoint_name)

        log_info(f"Creating: {endpoint_name}")

        try:
            spec = self._build_spec(config)

            # Debug logging for node group
            log_info(f"Node group in config: {config.get('node_group')}")
            log_info(f"Resource shape in config: {config.get('resource_shape')}")
            if "resource_requirement" in spec and "affinity" in spec["resource_requirement"]:
                log_info(f"Affinity in spec: {spec['resource_requirement']['affinity']}")
            else:
                log_info("No affinity found in spec")
            log_info(f"Full spec resource_requirement: {spec.get('resource_requirement', {})}")

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

    async def _handle_deletion(self, config: Dict[str, Any], task_template: TaskTemplate) -> LeptonMetadata:
        """Handle endpoint deletion"""
        endpoint_name = config.get("endpoint_name") or task_template.id.name
        endpoint_name = self._sanitize_endpoint_name(endpoint_name)

        log_info(f"Deleting: {endpoint_name}")

        try:
            # Execute deletion in thread pool (SDK is sync)
            await asyncio.get_event_loop().run_in_executor(None, lambda: self.deployment_api.delete(endpoint_name))

            log_info(f"Deleted: {endpoint_name}")
            return LeptonMetadata(deployment_name=endpoint_name)

        except Exception as e:
            log_info(f"Failed to delete: {e}")
            raise RuntimeError(f"Failed to delete Lepton endpoint: {e}")

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

            # Get recent logs to embed in message for better visibility
            log_message = f"Status: {state}"
            try:
                # Call our existing get_logs method to get recent logs
                log_response = self.get_logs(resource_meta, **kwargs)
                if log_response.logs:
                    # Get last few log lines for preview
                    recent_logs = log_response.logs[-3:]  # Last 3 lines
                    if recent_logs:
                        log_preview = " | ".join([line.strip() for line in recent_logs if line.strip()])
                        log_message = f"Status: {state} | Recent: {log_preview[:200]}..."  # Truncate for UI
            except Exception:
                # If log retrieval fails, just use basic status message
                pass

            # Return appropriate response
            if flyte_phase == TaskExecution.SUCCEEDED:
                # Get endpoint URL
                external_endpoint = "Unknown URL"
                if deployment.status and deployment.status.endpoint and deployment.status.endpoint.external_endpoint:
                    external_endpoint = deployment.status.endpoint.external_endpoint

                log_info(f"Ready: {external_endpoint}")
                return Resource(
                    phase=flyte_phase,
                    message=f"Ready: {external_endpoint} | {log_message}",
                    log_links=log_links,
                    outputs={"o0": external_endpoint},
                )
            else:
                log_info(f"Not ready: {state}")
                return Resource(
                    phase=flyte_phase,
                    message=log_message,
                    log_links=log_links,
                )

        except Exception as e:
            log_info(f"Error: {e}")

            # For deletion operations, a 404 error means successful deletion
            if "404" in str(e) and "not found" in str(e).lower():
                log_info(f"Deletion confirmed: {deployment_name} not found (404)")
                return Resource(
                    phase=TaskExecution.SUCCEEDED,
                    message=f"Successfully deleted: {deployment_name}",
                    outputs={"o0": f"Successfully deleted endpoint: {deployment_name}"},
                )

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


class LeptonEndpointDeploymentConnector(BaseLeptonConnector):
    """Connector for Lepton endpoint deployment operations."""

    def __init__(self):
        super().__init__(
            task_type_name="lepton_endpoint_deployment_task", connector_name="Lepton Endpoint Deployment Connector"
        )

    async def _handle_operation(self, config: Dict[str, Any], task_template: TaskTemplate) -> LeptonMetadata:
        """Handle deployment operations"""
        return await self._handle_deployment(config, task_template)


class LeptonEndpointDeletionConnector(BaseLeptonConnector):
    """Connector for Lepton endpoint deletion operations."""

    def __init__(self):
        super().__init__(
            task_type_name="lepton_endpoint_deletion_task", connector_name="Lepton Endpoint Deletion Connector"
        )

    async def _handle_operation(self, config: Dict[str, Any], task_template: TaskTemplate) -> LeptonMetadata:
        """Handle deletion operations"""
        return await self._handle_deletion(config, task_template)


# Register both connectors
ConnectorRegistry.register(LeptonEndpointDeploymentConnector())
ConnectorRegistry.register(LeptonEndpointDeletionConnector())
