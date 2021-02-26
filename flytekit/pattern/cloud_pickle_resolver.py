from base64 import b64decode, b64encode
from typing import List

import cloudpickle

from flytekit.core.context_manager import SerializationSettings
from flytekit.core.python_auto_container import PythonAutoContainerTask, TaskResolverMixin


class CloudPickleResolver(TaskResolverMixin):
    def name(self) -> str:
        return "cloud pickling task resolver"

    def load_task(self, loader_args: List[str]) -> PythonAutoContainerTask:
        raw_bytes = loader_args[0].encode("ascii")
        pickled = b64decode(raw_bytes)
        return cloudpickle.loads(pickled)

    def loader_args(self, settings: SerializationSettings, t: PythonAutoContainerTask) -> List[str]:
        return [b64encode(cloudpickle.dumps(t)).decode("ascii")]

    def get_all_tasks(self) -> List[PythonAutoContainerTask]:
        pass


default_cloud_pickle_resolver = CloudPickleResolver()
