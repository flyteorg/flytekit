import abc
from dataclasses import dataclass
from typing import Optional

from flyteidl.core import tasks_pb2


@dataclass
class BaseSelector(abc.ABC):
    value: Optional[str]

    @abc.abstractmethod
    def to_flyte_idl(self) -> tasks_pb2.Selector:
        pass


class GPUDeviceSelector(BaseSelector):
    def to_flyte_idl(self) -> tasks_pb2.Selector:
        return tasks_pb2.Selector(gpu_device=self.value)


class GPUPartitionSizeSelector(BaseSelector):
    def to_flyte_idl(self) -> tasks_pb2.Selector:
        return tasks_pb2.Selector(gpu_partition_size=self.value)


GPUDeviceA100 = GPUDeviceSelector(value="nvidia-tesla-a100")

GPUPartition1G5GB = GPUPartitionSizeSelector(value="1g.5gb")
GPUPartition2G10GB = GPUPartitionSizeSelector(value="2g.10gb")
GPUPartition3G20GB = GPUPartitionSizeSelector(value="3g.20gb")
GPUPartition4G20GB = GPUPartitionSizeSelector(value="4g.20gb")
GPUPartition7G40GB = GPUPartitionSizeSelector(value="7g.40gb")
GPUUnpartitioned = GPUPartitionSizeSelector(value=None)
