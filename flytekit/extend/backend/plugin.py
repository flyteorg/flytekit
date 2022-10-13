import typing


class BackendPluginBase():

    def __init__(self, identifier: str, task_type: str, version: str = "v1"):
        self._identifier = identifier
        self._task_type = task_type
        self._version = version

    @property
    def identifier(self) -> str:
        return self._identifier

    @property
    def task_type(self) -> str:
        return self._task_type

    @property
    def version(self) -> str:
        return self._version

    def initialize(self):
        pass

    async def create(self):
        pass

    async def poll(self):
        pass

    async def terminate(self):
        pass


class BackendPluginRegistry(object):
    @staticmethod
    def register(self, plugin: BackendPluginBase):
        pass

    @staticmethod
    def list_registered_plugins(self) -> typing.List[BackendPluginBase]:
        pass


