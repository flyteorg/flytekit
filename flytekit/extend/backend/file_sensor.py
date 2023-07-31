import asyncio

import fsspec
from fsspec.utils import get_protocol

from flytekit.configuration import DataConfig
from flytekit.core.data_persistence import s3_setup_args
from flytekit.extend.backend.base_agent import AgentRegistry
from flytekit.extend.backend.base_sensor import SensorBase


class FileSensor(SensorBase):
    def __init__(self):
        super().__init__(task_type="file_sensor")

    async def poke(self, path: str) -> bool:
        protocol = get_protocol(path)
        kwargs = {}
        if get_protocol(path):
            kwargs = s3_setup_args(DataConfig.auto().s3, anonymous=False)
        fs = fsspec.filesystem(protocol, **kwargs)
        return await asyncio.to_thread(fs.exists, path)


AgentRegistry.register(FileSensor())
