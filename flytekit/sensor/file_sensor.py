import asyncio

import fsspec
from fsspec.utils import get_protocol

from flytekit.configuration import DataConfig
from flytekit.core.data_persistence import s3_setup_args
from flytekit.sensor.base_sensor import BaseSensor


class FileSensor(BaseSensor):
    _TASK_TYPE = "file_sensor"

    def __init__(self, name: str, **kwargs):
        super().__init__(name=name, task_type=self._TASK_TYPE, **kwargs)

    async def poke(self, **kwargs) -> bool:
        path = cfg.path
        protocol = get_protocol(path)
        kwargs = {}
        if get_protocol(path):
            kwargs = s3_setup_args(DataConfig.auto().s3, anonymous=False)
        fs = fsspec.filesystem(protocol, **kwargs)
        return await asyncio.to_thread(fs.exists, path)
