from typing import Optional, TypeVar

from flytekit import FlyteContextManager
from flytekit.sensor.base_sensor import BaseSensor

T = TypeVar("T")


class FileSensor(BaseSensor):
    def __init__(self, name: str, config: Optional[T] = None, **kwargs):
        super().__init__(name=name, sensor_config=config, **kwargs)

    async def poke(self, path: str) -> bool:
        fs = FlyteContextManager.current_context().file_access.get_filesystem_for_path(path, asynchronous=True)
        return await fs._exists(path)
