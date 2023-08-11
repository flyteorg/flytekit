import asyncio
from typing import Optional, TypeVar

from flytekit import FlyteContextManager
from flytekit.sensor.base_sensor import BaseSensor

T = TypeVar("T")


class FileSensor(BaseSensor):
    def __init__(self, name: str, config: Optional[T] = None, **kwargs):
        super().__init__(name=name, sensor_config=config, **kwargs)

    async def poke(self, path: str) -> bool:
        ctx = FlyteContextManager.current_context()
        return await asyncio.to_thread(ctx.file_access.exists, path)
