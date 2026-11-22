from datetime import datetime
from typing import Any, Dict, List

from .utils import Aggregation

from flytekit.configuration import SerializationSettings
from flytekit.core.base_task import PythonTask
from flytekit.core.interface import Interface
from flytekit.extend.backend.base_agent import SyncAgentExecutorMixin


class InfluxDBTask(SyncAgentExecutorMixin, PythonTask):
    """
    This is a simple InfluxDB query task.
    You can retrieve a dataframe json with the ability to specify the bucket, measurement, start and end times,
    any fields or tags, a sampling period, and an aggregation method.
    """
    _TASK_TYPE = "influxdb"

    def __init__(self, name: str, url: str, org: str, **kwargs):
        """InfluxDB agent task.

        Args:
            name (str): Name of the task.
            url (str): InfluxDB server API url.
            org (str): InfluxDB organization name.
        """
        task_config = {"url": url, "org": org}

        inputs = {
            "bucket": str,
            "measurement": str,
            "start_time": datetime,
            "end_time": datetime,
            "fields": List[str],
            "tag_dict": Dict[str, List[str]],
            "period_min": int,
            "aggregation": Aggregation,
        }
        outputs = {"o0": str}

        super().__init__(
            task_type=self._TASK_TYPE,
            name=name,
            task_config=task_config,
            interface=Interface(inputs=inputs, outputs=outputs),
            **kwargs,
        )

    def get_custom(self, settings: SerializationSettings) -> Dict[str, Any]:
        return {
            "url": self.task_config["url"],
            "org": self.task_config["org"],
        }
