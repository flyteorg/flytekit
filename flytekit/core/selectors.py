import abc
from dataclasses import dataclass

from flyteidl.core import tasks_pb2


class BaseSelector(abc.ABC):
    @abc.abstractmethod
    def to_flyte_idl(self) -> tasks_pb2.Selector:
        pass


@dataclass
class GPUDeviceSelector(BaseSelector):
    value: str

    def to_flyte_idl(self) -> tasks_pb2.Selector:
        return tasks_pb2.Selector(gpu_device=self.value)


class GPUUnpartitionedSelector(BaseSelector):
    def to_flyte_idl(self) -> tasks_pb2.Selector:
        return tasks_pb2.Selector(gpu_unpartitioned=True)


@dataclass
class GPUPartitionSizeSelector(BaseSelector):
    value: str

    def to_flyte_idl(self) -> tasks_pb2.Selector:
        return tasks_pb2.Selector(gpu_partition_size=self.value)


GPUDeviceA100 = GPUDeviceSelector(value="nvidia-tesla-a100")
GPUUnpartitioned = GPUUnpartitionedSelector()
GPUPartition1G5GB = GPUPartitionSizeSelector(value="1g.5gb")
GPUPartition2G10GB = GPUPartitionSizeSelector(value="2g.10gb")
GPUPartition3G20GB = GPUPartitionSizeSelector(value="3g.20gb")
GPUPartition4G20GB = GPUPartitionSizeSelector(value="4g.20gb")
GPUPartition7G40GB = GPUPartitionSizeSelector(value="7g.40gb")
