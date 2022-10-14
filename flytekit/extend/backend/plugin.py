import typing
from abc import abstractmethod


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

    @abstractmethod
    async def initialize(self):
        pass

    @abstractmethod
    async def create(self):
        pass

    @abstractmethod
    async def poll(self):
        pass

    @abstractmethod
    async def terminate(self):
        pass


class BackendPluginRegistry(object):
    _REGISTRY = []

    @staticmethod
    def register(plugin: BackendPluginBase):
        BackendPluginRegistry._REGISTRY.append(plugin)

    @staticmethod
    def list_registered_plugins() -> typing.List[BackendPluginBase]:
        return BackendPluginRegistry._REGISTRY


class DummyPlugin(BackendPluginBase):
    def __init__(self, identifier="x"):
        super().__init__(identifier=identifier, task_type="my-task")

    async def initialize(self):
        return "Hello World"

    async def create(self):
        return "<h1>In Create</h1>"

    async def poll(self):
        return "<h1>In Poll</h1>"

    async def terminate(self):
        return "<h1>In Terminate</h1>"


for i in range(50):
    BackendPluginRegistry.register(DummyPlugin(f"x-{i}"))