"""Lepton AI endpoint task implementation."""

import os
import warnings
from typing import Any, Dict, Optional

from flytekit.configuration import SerializationSettings
from flytekit.core.base_task import PythonTask
from flytekit.core.interface import Interface
from flytekit.extend import TaskPlugins
from flytekit.extend.backend.base_connector import AsyncConnectorExecutorMixin

from .config import LeptonEndpointConfig


class LeptonEndpointDeploymentTask(AsyncConnectorExecutorMixin, PythonTask):
    """Task for Lepton AI endpoint deployment operations.

    This task follows standard Flytekit patterns by using a single configuration
    class that contains all necessary parameters.

    Args:
        config (LeptonEndpointConfig): Complete Lepton configuration
        **kwargs: Additional task parameters
    """

    _TASK_TYPE = "lepton_endpoint_deployment_task"

    def __init__(self, config: LeptonEndpointConfig, **kwargs):
        # Validate that we're running on Lepton platform
        self._validate_lepton_platform()

        # Create interface - no inputs needed since config has everything
        interface = Interface(
            inputs={},
            outputs={"o0": str},
        )

        # Use config directly
        task_name = kwargs.pop("name", "lepton_deployment_task")

        super().__init__(
            name=task_name,
            task_type=self._TASK_TYPE,
            task_config=config.to_dict(),
            interface=interface,
            **kwargs,
        )

    def _validate_lepton_platform(self):
        """Validate that this task can only run on Lepton platform."""
        # This could be enhanced to check environment variables or other platform indicators
        # For now, we'll add a basic check that can be expanded
        import os

        # Check for Lepton-specific environment indicators
        lepton_indicators = [
            "LEPTON_WORKSPACE_ID",
            "LEPTON_API_TOKEN",
            "LEPTON_PLATFORM",
            "DGXC_LEPTON_PLATFORM",  # Custom indicator
        ]

        # In production, you might want to be more strict
        # For development/testing, we'll be more permissive
        has_lepton_indicator = any(os.getenv(indicator) for indicator in lepton_indicators)

        # Allow override for development
        if os.getenv("LEPTON_PLATFORM_VALIDATION", "true").lower() == "false":
            return

        if not has_lepton_indicator:
            # This is a warning rather than hard error to allow development
            # In production, you might want to raise an exception
            import warnings

            warnings.warn(
                "Lepton endpoint tasks are designed to run on Lepton platform. "
                "Set DGXC_LEPTON_PLATFORM=true or disable validation with "
                "LEPTON_PLATFORM_VALIDATION=false for development.",
                UserWarning,
            )

    def get_custom(self, settings: SerializationSettings) -> Dict[str, Any]:
        """Return custom task attributes for serialization."""
        return self.task_config


def lepton_endpoint_deployment_task(config: LeptonEndpointConfig, task_name: Optional[str] = None) -> str:
    """Function for Lepton AI endpoint deployment.

    Args:
        config (LeptonEndpointConfig): Complete Lepton configuration including endpoint details
        task_name (Optional[str]): Optional custom task name

    Returns:
        str: Endpoint URL for successful deployment
    """
    # Create and execute the task
    task = LeptonEndpointDeploymentTask(config=config, name=task_name)
    return task()


class LeptonEndpointDeletionTask(AsyncConnectorExecutorMixin, PythonTask):
    """Task for Lepton AI endpoint deletion operations.

    This task only requires an endpoint name for simple deletions.

    Args:
        endpoint_name (str): Name of the endpoint to delete
        **kwargs: Additional task parameters
    """

    _TASK_TYPE = "lepton_endpoint_deletion_task"

    def __init__(self, endpoint_name: str, **kwargs):
        self._validate_lepton_platform()

        # Build minimal config for deletion - only endpoint name needed
        task_config = {
            "endpoint_name": endpoint_name,
        }

        interface = Interface(
            inputs={},
            outputs={"o0": str},
        )

        task_name = kwargs.pop("name", "lepton_deletion_task")

        super().__init__(
            name=task_name,
            task_type=self._TASK_TYPE,
            task_config=task_config,
            interface=interface,
            **kwargs,
        )

    def _validate_lepton_platform(self):
        """Validate that this task can only run on Lepton platform."""
        lepton_indicators = ["LEPTON_WORKSPACE_ID", "LEPTON_API_TOKEN", "LEPTON_PLATFORM", "DGXC_LEPTON_PLATFORM"]

        has_lepton_indicator = any(os.getenv(indicator) for indicator in lepton_indicators)

        if os.getenv("LEPTON_PLATFORM_VALIDATION", "true").lower() == "false":
            return

        if not has_lepton_indicator:
            warnings.warn(
                "Lepton tasks are designed to run on Lepton platform. "
                "Set DGXC_LEPTON_PLATFORM=true or disable validation with "
                "LEPTON_PLATFORM_VALIDATION=false for development.",
                UserWarning,
            )

    def get_custom(self, settings: SerializationSettings) -> Dict[str, Any]:
        """Return custom task attributes for serialization."""
        return self.task_config


def lepton_endpoint_deletion_task(endpoint_name: str, task_name: Optional[str] = None) -> str:
    """Function for Lepton AI endpoint deletion.

    Args:
        endpoint_name (str): Name of the endpoint to delete
        task_name (Optional[str]): Optional custom task name

    Returns:
        str: Success message confirming deletion
    """
    task = LeptonEndpointDeletionTask(endpoint_name=endpoint_name, name=task_name)
    return task()


# Register the Lepton endpoint plugins with Flytekit's dynamic plugin loading system
TaskPlugins.register_pythontask_plugin(LeptonEndpointConfig, LeptonEndpointDeploymentTask)
