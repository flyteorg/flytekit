import asyncio
import json
import logging
import re
import subprocess
import sys
import tempfile
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Optional

from flyteidl.admin.agent_pb2 import GetTaskLogsResponse
from flyteidl.core.execution_pb2 import TaskExecution, TaskLog

from flytekit import current_context
from flytekit.extend.backend.base_connector import AsyncConnectorBase, ConnectorRegistry, Resource, ResourceMeta
from flytekit.extend.backend.utils import convert_to_flyte_phase
from flytekit.loggers import logger
from flytekit.models.literals import LiteralMap
from flytekit.models.task import TaskTemplate


# Configure logging to ensure our messages go to stdout
def setup_logging():
    """Setup logging to ensure our connector logs are visible - respecting ROOT_LOG_LEVEL env var"""
    import os

    # Get log level from environment variable, default to DEBUG
    root_log_level = os.environ.get("ROOT_LOG_LEVEL", "DEBUG").upper()
    log_level_map = {
        "DEBUG": logging.DEBUG,
        "INFO": logging.INFO,
        "WARNING": logging.WARNING,
        "ERROR": logging.ERROR,
        "CRITICAL": logging.CRITICAL,
    }
    root_level = log_level_map.get(root_log_level, logging.DEBUG)

    root_logger = logging.getLogger()
    # Only add handler if none exists to avoid duplicates
    if not any(isinstance(h, logging.StreamHandler) and h.stream == sys.stdout for h in root_logger.handlers):
        handler = logging.StreamHandler(sys.stdout)
        handler.setFormatter(logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s"))
        root_logger.addHandler(handler)

    # Set root logger level based on environment variable
    root_logger.setLevel(root_level)
    print(f"ROOT LOGGER LEVEL SET TO: {root_log_level} ({root_level})")

    # Also configure the flytekit logger specifically for DEBUG
    flytekit_logger = logging.getLogger("flytekit")
    flytekit_logger.setLevel(logging.DEBUG)

    # Configure our connector logger for DEBUG
    connector_logger = logging.getLogger(__name__)
    connector_logger.setLevel(logging.DEBUG)

    return connector_logger


# Set up logging
connector_logger = setup_logging()


def log_info(message: str):
    """Helper function to log consistently across all channels"""
    print(message)
    sys.stdout.flush()
    logger.info(message)
    connector_logger.info(message)


def deep_merge(base: Dict[Any, Any], override: Dict[Any, Any]) -> Dict[Any, Any]:
    """Deep merge two dictionaries, with override taking precedence.
    Adapted from deployment_helpers.py
    """
    result = base.copy()

    for key, value in override.items():
        if key in result and isinstance(result[key], dict) and isinstance(value, dict):
            result[key] = deep_merge(result[key], value)
        else:
            result[key] = value

    return result


@dataclass
class LeptonMetadata(ResourceMeta):
    deployment_name: str


class LeptonEndpointConnector(AsyncConnectorBase):
    name = "Lepton Endpoint Connector"

    def __init__(self):
        super().__init__(task_type_name="lepton_endpoint_task", metadata_type=LeptonMetadata)

    def _sanitize_endpoint_name(self, name: str) -> str:
        """Sanitize endpoint name to meet Lepton requirements:
        - Lower case alphanumeric characters or '-' only
        - Must start with alphabetical character
        - Must end with alphanumeric character
        - Max length of 36 characters
        """
        # Convert to lowercase and replace invalid characters with hyphens
        sanitized = re.sub(r"[^a-z0-9\-]", "-", name.lower())

        # Ensure it starts with a letter
        if not sanitized[0].isalpha():
            sanitized = "ep-" + sanitized

        # Ensure it ends with alphanumeric
        sanitized = sanitized.rstrip("-")
        if not sanitized[-1].isalnum():
            sanitized = sanitized + "x"

        # Truncate to max length
        if len(sanitized) > 36:
            sanitized = sanitized[:33] + str(hash(name) % 100).zfill(2)

        return sanitized

    def _ensure_lep_cli_authenticated(self):
        """Ensure lep CLI is authenticated"""
        try:
            # Check if already logged in
            result = subprocess.run(
                ["lep", "workspace", "get"],
                capture_output=True,
                text=True,
                timeout=30,
            )
            if result.returncode == 0:
                logger.debug("lep CLI already authenticated")
                return
        except (subprocess.TimeoutExpired, subprocess.CalledProcessError, FileNotFoundError):
            pass

        # Try to authenticate using environment variables (set in agent deployment)
        try:
            import os

            workspace_id = os.environ.get("LEPTON_WORKSPACE_ID")
            token = os.environ.get("LEPTON_TOKEN")

            if not workspace_id or not token:
                # Fallback to Flyte secrets if env vars not available
                try:
                    secrets = current_context().secrets
                    workspace_id = secrets.get("lepton_workspace_id")
                    token = secrets.get("lepton_token")
                except Exception:
                    pass

            if not workspace_id or not token:
                raise ValueError(
                    "Missing required Lepton credentials: LEPTON_WORKSPACE_ID and LEPTON_TOKEN environment variables or lepton_workspace_id/lepton_token secrets"
                )

            # Login using credentials format: workspace_id:token
            credentials = f"{workspace_id}:{token}"
            result = subprocess.run(
                ["lep", "login", "-c", credentials],
                capture_output=True,
                text=True,
                timeout=60,
            )

            if result.returncode != 0:
                raise RuntimeError(f"Failed to authenticate with Lepton CLI: {result.stderr}")

            log_info(f"Successfully authenticated with Lepton CLI for workspace: {workspace_id}")

        except Exception as e:
            logger.error(f"Failed to authenticate with Lepton CLI: {e}")
            raise

    def _map_status_to_phase(self, status: str) -> TaskExecution.Phase:
        """Map Lepton endpoint status to Flyte execution phase using proper conversion."""
        # Convert status to lowercase for convert_to_flyte_phase
        status = status.lower() if status else "unknown"

        # Map Lepton states to states that convert_to_flyte_phase understands
        # Based on LeptonDeploymentState enum: Ready, NotReady, Starting, Updating, Deleting, Stopping, Stopped, Scaling, Unknown
        if status in ["starting", "scaling", "updating"]:
            return convert_to_flyte_phase("pending")  # Will convert to INITIALIZING
        elif status in ["not ready", "notready"]:
            return convert_to_flyte_phase("pending")  # "Not Ready" means still initializing
        elif status == "ready":
            return convert_to_flyte_phase("succeeded")  # Lepton "ready" = success
        elif status in ["deleting", "stopping", "terminating"]:
            return convert_to_flyte_phase("succeeded")  # Cleanup in progress
        elif status in ["stopped", "terminated"]:
            return convert_to_flyte_phase("succeeded")  # Stopped = failed
        elif status in ["unknown", "unk"]:
            return convert_to_flyte_phase("failed")  # Unknown = failed
        else:
            # For any other unknown status, default to pending to keep polling
            logger.warning(f"Unknown Lepton status: '{status}', defaulting to pending")
            return convert_to_flyte_phase("pending")

    def _extract_state_from_output(self, stdout: str) -> str:
        """Extract Lepton deployment state from lep endpoint status output"""
        import re

        # First try: "State: LeptonDeploymentState.Ready" format
        state_match = re.search(r"State:\s*LeptonDeploymentState\.(\w+)", stdout)
        if state_match:
            return state_match.group(1).lower()

        # Second try: "State: Ready" or "State: Not Ready" (handle multi-word states)
        state_fallback = re.search(r"State:\s*([A-Za-z\s]+)", stdout)
        if state_fallback:
            return state_fallback.group(1).strip().lower()

        # Third try: look for known Lepton states anywhere in the output
        known_states = [
            "ready",
            "not ready",
            "starting",
            "updating",
            "deleting",
            "stopping",
            "stopped",
            "scaling",
            "unk",
        ]
        for known_state in known_states:
            if known_state in stdout.lower():
                return known_state

        return "unknown"

    def _extract_endpoint_url(self, stdout: str, deployment_name: str) -> str:
        """Extract external endpoint URL from lep endpoint status JSON output"""
        try:
            # Look for the JSON part in the output (after the table)
            import re

            json_match = re.search(r"\{.*\}", stdout, re.DOTALL)
            if json_match:
                json_str = json_match.group(0)
                # Parse the JSON to extract external_endpoint
                import ast

                status_data = ast.literal_eval(json_str)
                if "status" in status_data and "endpoint" in status_data["status"]:
                    external_endpoint = status_data["status"]["endpoint"].get("external_endpoint", "")
                    if external_endpoint:
                        log_info(f"Extracted endpoint URL: {external_endpoint}")
                        return external_endpoint
        except Exception as e:
            log_info(f"Failed to extract endpoint URL: {e}")

        # Fallback: use regex to find any https URL
        url_match = re.search(r'https://[^\s\'"]+', stdout)
        if url_match:
            url = url_match.group(0)
            log_info(f"Extracted URL via regex: {url}")
            return url

        # Final fallback
        return f"Lepton endpoint {deployment_name} is ready"

    def _create_container_spec(self, custom_config: Dict[str, Any]) -> Dict[str, Any]:
        """Create container specification from deployment config."""
        deployment_type = custom_config.get("deployment_type", "custom")
        port = custom_config.get("port", 8080)

        container_spec = {
            "image": custom_config.get("image", "python:3.11-slim"),  # Default image if none specified
            "ports": [{"container_port": port}],
        }

        # Generate command based on deployment type
        if deployment_type == "vllm":
            # vLLM deployment
            checkpoint_path = custom_config.get("checkpoint_path", "meta-llama/Llama-3.1-8B-Instruct")
            served_model_name = custom_config.get("served_model_name", "default-model")
            tensor_parallel_size = int(custom_config.get("tensor_parallel_size", 1))
            pipeline_parallel_size = int(custom_config.get("pipeline_parallel_size", 1))
            data_parallel_size = int(custom_config.get("data_parallel_size", 1))

            command_parts = [
                "vllm",
                "serve",
                checkpoint_path,
                f"--tensor-parallel-size={tensor_parallel_size}",
                f"--pipeline-parallel-size={pipeline_parallel_size}",
                f"--data-parallel-size={data_parallel_size}",
                f"--port={int(port)}",
                f"--served-model-name={served_model_name}",
            ]

            # Add extra args if provided
            if custom_config.get("extra_args"):
                command_parts.extend(custom_config["extra_args"].split())

            container_spec["command"] = command_parts

        elif deployment_type == "sglang":
            # SGLang deployment
            checkpoint_path = custom_config.get("checkpoint_path", "meta-llama/Llama-3.1-8B-Instruct")
            served_model_name = custom_config.get("served_model_name", "default-model")
            tensor_parallel_size = int(custom_config.get("tensor_parallel_size", 1))
            data_parallel_size = int(custom_config.get("data_parallel_size", 1))

            command_parts = [
                "python3",
                "-m",
                "sglang.launch_server",
                f"--model-path={checkpoint_path}",
                "--host=0.0.0.0",
                f"--port={int(port)}",
                f"--served-model-name={served_model_name}",
                f"--tp={tensor_parallel_size}",
                f"--dp={data_parallel_size}",
            ]

            # Add extra args if provided
            if custom_config.get("extra_args"):
                command_parts.extend(custom_config["extra_args"].split())

            container_spec["command"] = command_parts

        elif deployment_type == "nim":
            # NIM containers use their default entrypoint - no custom command needed
            # Configuration is handled via environment variables
            pass

        elif deployment_type == "custom":
            # For custom deployments, use user-specified command or default
            if custom_config.get("command"):
                container_spec["command"] = custom_config["command"]
            else:
                # Default to a simple HTTP server if no command specified
                container_spec["command"] = ["/bin/bash", "-c", f"python3 -m http.server {int(port)} --bind 0.0.0.0"]

        # For other custom types, assume the container has its own entrypoint

        return container_spec

    def _add_deployment_derived_envs(self, envs_list: list, custom_config: Dict[str, Any]) -> None:
        """Add environment variables derived from deployment configuration."""
        deployment_type = custom_config.get("deployment_type", "custom")

        # Common environment variables for all deployment types
        if custom_config.get("served_model_name"):
            envs_list.append({"name": "SERVED_MODEL_NAME", "value": custom_config["served_model_name"]})

        if custom_config.get("port"):
            envs_list.append({"name": "MODEL_PORT", "value": str(int(custom_config["port"]))})

        # Deployment-specific environment variables
        if deployment_type == "vllm":
            if custom_config.get("checkpoint_path"):
                envs_list.append({"name": "MODEL_PATH", "value": custom_config["checkpoint_path"]})
            if custom_config.get("tensor_parallel_size") is not None:
                envs_list.append(
                    {"name": "TENSOR_PARALLEL_SIZE", "value": str(int(custom_config["tensor_parallel_size"]))}
                )

        elif deployment_type == "sglang":
            if custom_config.get("checkpoint_path"):
                envs_list.append({"name": "MODEL_PATH", "value": custom_config["checkpoint_path"]})
            if custom_config.get("tensor_parallel_size") is not None:
                envs_list.append(
                    {"name": "TENSOR_PARALLEL_SIZE", "value": str(int(custom_config["tensor_parallel_size"]))}
                )

        elif deployment_type == "nim":
            # NIM-specific derived environment variables
            if custom_config.get("served_model_name"):
                envs_list.append({"name": "NIM_MODEL_NAME", "value": custom_config["served_model_name"]})

    def _generate_endpoint_spec(self, task_template: TaskTemplate) -> Dict[str, Any]:
        """Generate endpoint specification from task template."""
        # task_template.custom is already a dict (converted during TaskTemplate.from_flyte_idl)
        # so we can use it directly instead of calling MessageToDict again
        if task_template.custom:
            custom_config = task_template.custom
        else:
            custom_config = {}

        log_info(f"Processing custom config for deployment type: {custom_config.get('deployment_type', 'custom')}")

        # Step 1: Start with base config (platform defaults)
        base_config = {}

        # Add queue config from platform defaults if specified
        if custom_config.get("queue_config"):
            base_config["queue_config"] = custom_config["queue_config"]

        # Step 2: Create container specification
        container_spec = self._create_container_spec(custom_config)

        # Step 3: Create deployment config
        deployment_config = {
            "resource_requirement": {
                **base_config.get("resource_requirement", {}),
                "resource_shape": custom_config.get("resource_shape", "cpu.small"),
                "min_replicas": custom_config.get("min_replicas", 1),
                "max_replicas": custom_config.get("max_replicas", 1),
            },
            "auto_scaler": custom_config.get(
                "auto_scaler",
                {
                    "scale_down": {"no_traffic_timeout": 0, "scale_from_zero": False},
                    "target_gpu_utilization_percentage": 0,
                    "target_throughput": {"qpm": 2.5, "paths": [], "methods": []},
                },
            ),
            "container": container_spec,
            "envs": [],
        }

        # Step 4: Add health check configuration if provided (following deployment_helpers pattern)
        if custom_config.get("health_check"):
            deployment_config["health"] = custom_config["health_check"]

        # Step 5: Merge base config with deployment config (following deployment_helpers pattern)
        final_config = deep_merge(base_config, deployment_config)

        # Step 6: Add node group affinity to resource_requirement (following deployment_helpers pattern)
        if custom_config.get("node_group"):
            if "resource_requirement" not in final_config:
                final_config["resource_requirement"] = {}
            final_config["resource_requirement"]["affinity"] = {
                "allowed_dedicated_node_groups": [custom_config["node_group"]]
            }

        # Step 7: Add deployment-derived environment variables (following deployment_helpers pattern)
        self._add_deployment_derived_envs(final_config["envs"], custom_config)

        # Step 8: Add user-defined environment variables (following deployment_helpers pattern)
        env_vars = custom_config.get("env_vars", {})
        for key, value in env_vars.items():
            env_var = {"name": key}

            # Support both direct values and secret references
            if isinstance(value, dict) and "value_from" in value:
                # Secret reference: {"value_from": {"secret_name_ref": "secret_name"}}
                env_var["value_from"] = dict(value["value_from"])
            else:
                # Direct value: "direct_value"
                env_var["value"] = str(value)

            final_config["envs"].append(env_var)

        # Step 9: Add API tokens (following deployment_helpers pattern)
        api_tokens = custom_config.get("api_tokens", [])
        if api_tokens:
            api_tokens_list = []
            for token_config in api_tokens:
                token_var = {}

                # Support both direct values and secret references
                if isinstance(token_config, dict):
                    if "value" in token_config:
                        # Direct value: {"value": "token_string"}
                        token_var["value"] = str(token_config["value"])
                    elif "value_from" in token_config:
                        # Secret reference: {"value_from": {"token_name_ref": "secret_name"}}
                        if "secret_name_ref" in token_config["value_from"]:
                            token_var["value_from"] = {"token_name_ref": token_config["value_from"]["secret_name_ref"]}
                        else:
                            token_var["value_from"] = dict(token_config["value_from"])
                else:
                    # Simple string value
                    token_var["value"] = str(token_config)

                api_tokens_list.append(token_var)

            final_config["api_tokens"] = api_tokens_list

        # Backward compatibility: support legacy single api_token (following deployment_helpers pattern)
        elif custom_config.get("api_token"):
            final_config["api_tokens"] = [{"value": custom_config["api_token"]}]

        # Step 10: Add storage mounts with intelligent path construction (following deployment_helpers pattern)
        mounts_config = custom_config.get("mounts", {})
        if mounts_config and mounts_config.get("enabled", False):
            # Get storage source from task config mounts (since mounts are shared between tasks and deployments)
            storage_source = mounts_config.get("storage_source", "node-nfs:lepton-shared-fs")

            final_config["mounts"] = [
                {
                    "path": mounts_config["cache_path"],
                    "from": storage_source,
                    "mount_path": mounts_config["mount_path"],
                    "mount_options": {},
                }
            ]

        # Step 11: Extract image_pull_secrets to top level (required by Lepton API, following deployment_helpers pattern)
        image_pull_secrets = custom_config.get("image_pull_secrets", [])
        if image_pull_secrets:
            # Convert to regular Python list
            final_config["image_pull_secrets"] = list(image_pull_secrets)

        # Step 12: Add additional required fields (following working JSON examples)
        if "health" not in final_config:
            final_config["health"] = {}  # Empty like in working examples
        final_config["log"] = {"enable_collection": True}
        final_config["metrics"] = {}
        final_config["routing_policy"] = {}
        final_config["enable_rdma"] = False
        final_config["user_security_context"] = {}

        return final_config

    async def create(
        self, task_template: TaskTemplate, inputs: Optional[LiteralMap] = None, **kwargs
    ) -> LeptonMetadata:
        """Deploy a Lepton inference endpoint or handle deletion using lep CLI"""

        log_info(f"Processing Lepton task: {task_template.id}")
        log_info(f"Task template custom config: {task_template.custom}")

        # Ensure CLI authentication
        self._ensure_lep_cli_authenticated()

        # Handle normal endpoint creation
        return await self._handle_creation_task(task_template)

    async def _handle_creation_task(self, task_template: TaskTemplate) -> LeptonMetadata:
        """Handle normal endpoint creation task."""

        log_info(f"Creating Lepton endpoint for task: {task_template.id}")

        # Generate unique deployment name and sanitize it for Lepton requirements
        raw_name = f"{task_template.id.name}-{int(time.time())}"
        deployment_name = self._sanitize_endpoint_name(raw_name)
        log_info(f"Sanitized endpoint name: '{raw_name}' -> '{deployment_name}'")

        # Generate endpoint specification
        spec = self._generate_endpoint_spec(task_template)

        # Write spec to temporary file
        with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
            json.dump(spec, f, indent=2)
            spec_file = f.name

        try:
            logger.info(f"Creating Lepton endpoint: {deployment_name}")

            # Create endpoint using lep CLI
            logger.info(f"Creating endpoint with spec file: {spec_file}")
            logger.debug(f"Endpoint spec content: {json.dumps(spec, indent=2)}")

            result = await asyncio.get_event_loop().run_in_executor(
                None,
                lambda: subprocess.run(
                    ["lep", "endpoint", "create", "--file", spec_file, "--name", deployment_name],
                    capture_output=True,
                    text=True,
                    timeout=300,
                ),
            )

            log_info(f"lep endpoint create result: returncode={result.returncode}")
            log_info(f"stdout: {result.stdout}")
            log_info(f"stderr: {result.stderr}")

            if result.returncode == 0:
                log_info(f"Successfully created Lepton endpoint: {deployment_name}")
                metadata = LeptonMetadata(deployment_name=deployment_name)
                log_info(f"Returning metadata: {metadata}")
                return metadata
            else:
                error_msg = result.stderr or result.stdout or "Unknown error"
                logger.error(f"Failed to create endpoint: {error_msg}")
                raise RuntimeError(f"Failed to create Lepton endpoint: {error_msg}")

        except subprocess.TimeoutExpired:
            logger.error(f"Timeout creating Lepton endpoint: {deployment_name}")
            raise RuntimeError(f"Timeout creating Lepton endpoint: {deployment_name}")
        except Exception as e:
            logger.error(f"Error creating Lepton endpoint: {e}")
            raise
        finally:
            # Clean up temporary file
            Path(spec_file).unlink(missing_ok=True)

    async def get(self, resource_meta: LeptonMetadata, **kwargs) -> Resource:
        """Get the status of a Lepton endpoint using lep CLI"""
        import os

        workspace_id = os.environ.get("LEPTON_WORKSPACE_ID")

        deployment_name = resource_meta.deployment_name
        log_info(f"Checking status for deployment: {deployment_name}")
        log_info(f"Resource metadata: {resource_meta}")

        try:
            # First, ensure we're authenticated (status checks might fail if not)
            self._ensure_lep_cli_authenticated()
            log_info("Authentication check completed")

            # Get endpoint status using lep CLI with --detail for URL extraction
            log_info(f"Checking endpoint status: {deployment_name}")
            result = await asyncio.get_event_loop().run_in_executor(
                None,
                lambda: subprocess.run(
                    ["lep", "endpoint", "status", "--detail", "--name", deployment_name],
                    capture_output=True,
                    text=True,
                    timeout=30,
                ),
            )

            log_info(f"Endpoint status result: returncode={result.returncode}")
            log_info(f"STDOUT: {result.stdout}")
            log_info(f"STDERR: {result.stderr}")

            if result.returncode != 0:
                log_info(f"Failed to get Lepton endpoint status: {result.stderr}")
                return Resource(phase=convert_to_flyte_phase("failed"))

            # Extract state from text output
            state = self._extract_state_from_output(result.stdout)
            log_info(f"Extracted state: '{state}'")

            # Map to Flyte phase
            flyte_phase = self._map_status_to_phase(state)
            log_info(f"Final Flyte phase: {flyte_phase}")

            log_links = [
                TaskLog(
                    uri=f"https://dashboard.dgxc-lepton.nvidia.com/workspace/{workspace_id}/compute/deployments/detail/{deployment_name}/replicas/list#/deployment/{deployment_name}/logs",
                    name="Lepton Console",
                )
            ]

            # Return appropriate response based on state
            if flyte_phase == TaskExecution.SUCCEEDED:
                # Extract external endpoint URL from the JSON output
                external_endpoint = self._extract_endpoint_url(result.stdout, deployment_name)
                log_info(f"Endpoint ready! State: {state}, URL: {external_endpoint}")

                # Return both URL and endpoint name for potential deletion
                output_message = f"URL: {external_endpoint} | Endpoint: {deployment_name}"
                return Resource(
                    phase=flyte_phase,
                    message=f"Endpoint status: {state}",
                    log_links=log_links,
                    outputs={"o0": output_message},
                )
            else:
                log_info(f"Endpoint not ready yet. State: {state}, continuing to poll")
                return Resource(
                    phase=flyte_phase,
                    message=f"Endpoint status: {state}",
                    log_links=log_links,
                )

        except (subprocess.TimeoutExpired, json.JSONDecodeError) as e:
            log_info(f"Failed to get status for Lepton endpoint {deployment_name}: {e}")
            return Resource(phase=convert_to_flyte_phase("failed"))
        except Exception as e:
            log_info(f"Unexpected error getting Lepton endpoint status {deployment_name}: {e}")
            return Resource(phase=convert_to_flyte_phase("failed"))

    async def delete(self, resource_meta: LeptonMetadata, **kwargs):
        """Delete a Lepton endpoint using lep CLI"""

        deployment_name = resource_meta.deployment_name

        try:
            log_info(f"Deleting Lepton endpoint: {deployment_name}")

            # Delete endpoint using lep CLI
            result = await asyncio.get_event_loop().run_in_executor(
                None,
                lambda: subprocess.run(
                    ["lep", "endpoint", "remove", "--name", deployment_name],
                    capture_output=True,
                    text=True,
                    timeout=60,
                ),
            )

            if result.returncode == 0:
                log_info(f"Successfully deleted Lepton endpoint: {deployment_name}")
            else:
                # Check if endpoint doesn't exist (idempotent deletion)
                if "not found" in result.stderr.lower() or "404" in result.stderr:
                    log_info(f"Endpoint {deployment_name} already deleted or doesn't exist - treating as success")
                else:
                    logger.error(f"Failed to delete Lepton endpoint: {result.stderr}")
                    raise RuntimeError(f"Failed to delete Lepton endpoint: {result.stderr}")

        except subprocess.TimeoutExpired:
            logger.error(f"Timeout deleting Lepton endpoint: {deployment_name}")
            raise RuntimeError(f"Timeout deleting Lepton endpoint: {deployment_name}")
        except Exception as e:
            logger.error(f"Error deleting Lepton endpoint: {e}")
            raise

    def get_logs(self, resource_meta: LeptonMetadata, **kwargs) -> GetTaskLogsResponse:
        """Get logs from Lepton endpoint using lep CLI"""

        deployment_name = resource_meta.deployment_name

        try:
            log_info(f"Getting logs for Lepton endpoint: {deployment_name}")

            # Get logs using lep CLI
            result = subprocess.run(
                ["lep", "log", "get", "--deployment", deployment_name, "--limit", "1000"],
                capture_output=True,
                text=True,
                timeout=30,
            )

            if result.returncode == 0:
                # Parse log lines and create TaskLog entries
                log_lines = result.stdout.strip().split("\n") if result.stdout.strip() else []

                # Create task logs
                task_logs = []
                for line in log_lines:
                    if line.strip():
                        # Create a TaskLog entry for each log line
                        task_log = TaskLog(uri=f"lepton://endpoint/{deployment_name}/logs", name="lepton-endpoint-logs")
                        task_logs.append(task_log)
                        break  # Just add one log link for now

                return GetTaskLogsResponse(
                    logs=log_lines,
                    token="",  # No pagination token needed
                    log_links=task_logs,
                )
            else:
                logger.error(f"Failed to get logs for endpoint {deployment_name}: {result.stderr}")
                return GetTaskLogsResponse(logs=[], token="", log_links=[])

        except Exception as e:
            logger.error(f"Error getting logs for endpoint {deployment_name}: {e}")
            return GetTaskLogsResponse(logs=[], token="", log_links=[])


# Register the endpoint connector
ConnectorRegistry.register(LeptonEndpointConnector())

# Backward compatibility alias
LeptonConnector = LeptonEndpointConnector
