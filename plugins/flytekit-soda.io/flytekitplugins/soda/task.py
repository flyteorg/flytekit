"""
This Plugin integrates Soda.io data quality checks with Flyte, allowing users to run and manage
data quality scans as part of Flyte workflows. It leverages Soda.io's scanning capabilities to monitor
data quality by defining customizable scan configurations.

This plugin allows setting various parameters like scan definition files, data sources, and Soda Cloud
API credentials to run these scans in an automated fashion within Flyte.
"""
from dataclasses import dataclass
from flytekit import PythonFunctionTask
from typing import Any, Dict, Callable, Optional

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
        # Example code to invoke Soda.io data scan
        scan_definition = self.task_config.scan_definition
        # Integrate with Soda.io here, such as by calling a scan API with scan_definition

        # You would actually integrate with the Soda API here, using soda-cloud-api-key and other configurations
        result = {}  # Placeholder for API result

        return {"scan_result": result}