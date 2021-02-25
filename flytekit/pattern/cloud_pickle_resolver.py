from base64 import b64decode, b64encode
from typing import List

import cloudpickle

from flytekit.core.context_manager import SerializationSettings
from flytekit.core.python_auto_container import TaskResolverMixin, PythonAutoContainerTask


class CloudPickleResolver(TaskResolverMixin):
    @classmethod
    def name(cls) -> str:
        return "cloud pickling task resolver"

    @classmethod
    def load_task(cls, loader_args: List[str]) -> PythonAutoContainerTask:
        raw_bytes = loader_args[0].encode("ascii")
        pickled = b64decode(raw_bytes)
        return cloudpickle.loads(pickled)

    @classmethod
    def loader_args(cls, settings: SerializationSettings, t: PythonAutoContainerTask) -> List[str]:
        return [b64encode(cloudpickle.dumps(t)).decode("ascii")]

    @classmethod
    def get_all_tasks(cls) -> List[PythonAutoContainerTask]:
        pass
