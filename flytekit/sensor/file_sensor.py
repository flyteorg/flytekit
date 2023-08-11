import asyncio
from typing import Optional, TypeVar

import fsspec
from fsspec.utils import get_protocol

from flytekit.configuration import DataConfig
from flytekit.core.data_persistence import s3_setup_args
from flytekit.sensor.base_sensor import BaseSensor

T = TypeVar("T")


class FileSensor(BaseSensor):
    def __init__(self, name: str, config: Optional[T] = None, **kwargs):
        super().__init__(name=name, sensor_config=config, **kwargs)

    async def poke(self, path: str) -> bool:
        protocol = get_protocol(path)
        kwargs = {}
        if get_protocol(path):
            kwargs = s3_setup_args(DataConfig.auto().s3, anonymous=False)
        fs = fsspec.filesystem(protocol, **kwargs)
        return await asyncio.to_thread(fs.exists, path)
