"""
This Plugin integrates Soda.io data quality checks with Flyte, allowing users to run and manage
data quality scans as part of Flyte workflows. It leverages Soda.io's scanning capabilities to monitor
data quality by defining customizable scan configurations.

This plugin allows setting various parameters like scan definition files, data sources, and Soda Cloud
API credentials to run these scans in an automated fashion within Flyte.
"""

import os
import subprocess
from dataclasses import dataclass
from typing import Any, Callable, Dict, Optional

import requests

from flytekit import PythonFunctionTask
from flytekit.configuration import SecretsManager


# This would be the main task configuration class for Soda.io
@dataclass
class SodaCheckConfig:
    """
    Configuration class for a Soda.io data quality scan task.

    Attributes:
        scan_definition (str): Path to the Soda.io scan definition YAML file.
        soda_cloud_api_key (Optional[str]): API key for Soda Cloud access, if applicable.
        data_source (Optional[str]): Name of the data source in Soda.io to use for the scan.
        scan_name (Optional[str]): Name of the scan job for organizational purposes.
    """

    scan_definition: str
    soda_cloud_api_key: Optional[str] = None
    data_source: Optional[str] = None  # Name of the data source in Soda.io
    scan_name: Optional[str] = "default_scan"  # Name for the scan job


class SodaCheckTask(PythonFunctionTask[SodaCheckConfig]):
    """
    A Flyte task that runs a Soda.io data quality scan as defined in the provided configuration.

    This task allows users to execute data quality checks by leveraging the Soda.io API. The scan
    configuration includes options for specifying the scan definition file, Soda.io Cloud API key,
    and data source.

    Attributes:
        _TASK_TYPE (str): The task type identifier for Soda.io checks within Flyte.
    """

    _TASK_TYPE = "soda_check_task"

    def __init__(self, task_config: SodaCheckConfig, task_function: Callable, **kwargs):
        """
        Initializes the SodaCheckTask with the provided configuration.

        Args:
            task_config (SodaCheckConfig): The configuration for the Soda.io scan.
            task_function (Callable): The function representing the task logic.
            kwargs: Additional keyword arguments.
        """
        super().__init__(
            task_type=self._TASK_TYPE,
            task_config=task_config,
            task_function=task_function,
            task_type_version=1,
            **kwargs,
        )

    def execute(self, **kwargs) -> Dict[str, Any]:
        """
        Executes the Soda.io scan using the configuration provided in task_config.

        Returns:
            dict: A dictionary containing the results of the Soda.io scan.
        """
        # Retrieve the Soda Cloud API key from environment or Flyte's SecretsManager
        api_key = (
            self.task_config.soda_cloud_api_key
            or os.getenv("SODA_CLOUD_API_KEY")
            or SecretsManager.get_secrets("soda", "api_key")
        )

        if not api_key:
            raise ValueError("Soda Cloud API key is required but not provided.")

        scan_definition = self.task_config.scan_definition
        data_source = self.task_config.data_source
        scan_name = self.task_config.scan_name

        # Placeholder for API request to Soda.io
        url = "https://api.soda.io/v1/scan"  # Replace with actual Soda.io API endpoint

        # Prepare the request payload
        payload = {
            "scan_definition": scan_definition,
            "data_source": data_source,
            "scan_name": scan_name,
            "api_key": api_key,
        }

        # Placeholder for API result
        result = {}

        # Make the API call (using POST method as an example)
        try:
            response = requests.post(url, json=payload)
            response.raise_for_status()  # Raise an error for bad responses (4xx or 5xx)

            # Assuming the API returns a JSON response
            result = response.json()

        except requests.exceptions.RequestException as e:
            raise RuntimeError(f"API call failed: {e}")

        return {"scan_result": result}
