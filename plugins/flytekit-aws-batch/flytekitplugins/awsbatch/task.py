import os
import typing
from dataclasses import dataclass
from typing import Any, Callable, Dict, Optional

from flytekit import FlyteContextManager, PythonFunctionTask, TaskMetadata
from flytekit.core.map_task import MapPythonTask
from flytekit.extend import ExecutionState, SerializationSettings, TaskPlugins
from flytekit.models.array_job import ArrayJob


@dataclass
class AWSBatch(object):
    """
    Use this to configure a job definition for a AWS batch job. Task's marked with this will automatically execute
    natively onto AWS batch service

    Args:
        job_definition: Dictionary of job definition. The variables should match what AWS batch expects
        concurrency: If specified, this limits the number of mapped tasks than can run in parallel to the given batch size
        min_success_ratio: If specified, this determines the minimum fraction of total jobs which can complete
        successfully before terminating this task and marking it successful.
    """

    job_definition: Optional[Dict[str, str]] = None
    concurrency: int = 0
    min_success_ratio: float = 0

    def __post_init__(self):
        if self.job_definition is None:
            self.job_definition = {}


class AWSBatchFunctionTask(MapPythonTask):
    """
    Actual Plugin that transforms the local python code for execution within AWS batch job
    """

    # _AWS_BATCH_TASK_TYPE = "aws-batch"

    def __init__(self, aws_batch_config: AWSBatch, task_function: Callable, **kwargs):
        super(AWSBatchFunctionTask, self).__init__(
            python_function_task=PythonFunctionTask(task_config=None, task_function=task_function, **kwargs),
        )
        self._task_function = task_function
        self._aws_batch_config = aws_batch_config

    @property
    def task_function(self):
        return self._task_function

    @property
    def aws_batch_config(self):
        return self._aws_batch_config

    # def get_config(self, settings: SerializationSettings) -> Dict[str, str]:
    #     return self.task_config.job_definition


# Inject the AWS batch plugin into flytekits dynamic plugin loading system
TaskPlugins.register_pythontask_plugin(AWSBatch, AWSBatchFunctionTask)
