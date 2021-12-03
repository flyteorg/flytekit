from dataclasses import dataclass
from typing import Any, Callable, Dict, List, Optional, Union

from dataclasses_json import dataclass_json
from google.protobuf import json_format
from google.protobuf.struct_pb2 import Struct

from flytekit import PythonFunctionTask, TaskMetadata
from flytekit.extend import SerializationSettings, TaskPlugins


@dataclass_json
@dataclass
class AWSBatch(object):
    """
    Use this to configure a job definition for a AWS batch job. Task's marked with this will automatically execute
    natively onto AWS batch service.
    Refer to AWS job definition template for more detail: https://docs.aws.amazon.com/batch/latest/userguide/job-definition-template.html
    """

    parameters: Optional[Dict[str, str]] = None
    schedulingPriority: Optional[int] = None
    PlatformCapabilities: Optional[List[str]] = None
    PropagateTags: Optional[bool] = None
    RetryStrategy: Optional[Dict[str, Union[str, int, dict]]] = None
    Tags: Optional[Dict[str, str]] = None
    Timeout: Optional[Dict[str, int]] = None


class AWSBatchFunctionTask(PythonFunctionTask):
    """
    Actual Plugin that transforms the local python code for execution within AWS batch job
    """

    _AWS_BATCH_TASK_TYPE = "aws-batch"

    def __init__(self, task_config: AWSBatch, task_function: Callable, **kwargs):
        if task_config is None:
            task_config = AWSBatch()
        super(AWSBatchFunctionTask, self).__init__(
            task_config=task_config, task_type=self._AWS_BATCH_TASK_TYPE, task_function=task_function, **kwargs
        )
        self._run_task = PythonFunctionTask(task_config=None, task_function=task_function)
        self._task_config = task_config

    def get_custom(self, settings: SerializationSettings) -> Dict[str, Any]:
        s = Struct()
        s.update(self._task_config.to_dict())
        return json_format.MessageToDict(s)

    def get_command(self, settings: SerializationSettings) -> List[str]:
        container_args = [
            "pyflyte-map-execute",
            "--inputs",
            "{{.input}}",
            "--output-prefix",
            "{{.outputPrefix}}",
            "--raw-output-data-prefix",
            "{{.rawOutputDataPrefix}}",
            "--is-aws-batch-single-job",
            "--resolver",
            self._run_task.task_resolver.location,
            "--",
            *self._run_task.task_resolver.loader_args(settings, self._run_task),
        ]

        return container_args


# Inject the AWS batch plugin into flytekits dynamic plugin loading system
TaskPlugins.register_pythontask_plugin(AWSBatch, AWSBatchFunctionTask)
