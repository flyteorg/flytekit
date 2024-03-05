from flytekit import FlyteContextManager
from flytekit.sensor.base_sensor import BaseSensor


class FileSensor(BaseSensor):
    def __init__(self, name: str, **kwargs):
        super().__init__(name=name, **kwargs)

    async def poke(self, path: str) -> bool:
        file_access = FlyteContextManager.current_context().file_access
        fs = file_access.get_filesystem_for_path(path, asynchronous=True)
        if file_access.is_remote(path):
            return await fs._exists(path)
        return fs.exists(path)
