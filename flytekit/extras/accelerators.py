import abc
import copy
import enum
from typing import Generic, Optional, Type, TypeVar

from flyteidl.core import tasks_pb2

T = TypeVar("T")
ACC = TypeVar("ACC", bound="BaseAccelerator")
MIG = TypeVar("MIG", bound="MultiInstanceGPUAccelerator")
PART = TypeVar("PART", bound=enum.Enum)


class BaseAccelerator(abc.ABC, Generic[T]):
    @abc.abstractmethod
    def to_flyte_idl(self) -> T:
        ...


class GPUAccelerator(BaseAccelerator):
    def __init__(self, device: str) -> None:
        self._device = device

    def to_flyte_idl(self) -> tasks_pb2.GPUAccelerator:
        return tasks_pb2.GPUAccelerator(device=self._device)


NvidiaA10G = GPUAccelerator("nvidia-a10g")
NvidiaL4 = GPUAccelerator("nvidia-l4-vws")
NvidiaTeslaK80 = GPUAccelerator("nvidia-tesla-k80")
NvidiaTeslaM60 = GPUAccelerator("nvidia-tesla-m60")
NvidiaTeslaP4 = GPUAccelerator("nvidia-tesla-p4")
NvidiaTeslaP100 = GPUAccelerator("nvidia-tesla-p100")
NvidiaTeslaT4 = GPUAccelerator("nvidia-tesla-t4")
NvidiaTeslaV100 = GPUAccelerator("nvidia-tesla-v100")


class MultiInstanceGPUAccelerator(BaseAccelerator, Generic[PART]):
    _partition_size: Optional[PART]

    def __init__(self, device: str, partition_sizes: Type[PART]) -> None:
        self._device = device
        self._partition_sizes = partition_sizes

    @property
    def partition_sizes(self) -> Type[PART]:
        return self._partition_sizes

    def with_partition_size(self: MIG, partition_size: Optional[PART]) -> MIG:
        if partition_size is not None and partition_size not in self._partition_sizes:
            raise ValueError(
                f"Invalid partition size for device {self._device!r}. Expected one of ({', '.join(map(str, self._partition_sizes))}), but got: {partition_size}"
            )
        instance = copy.deepcopy(self)
        instance._partition_size = partition_size
        return instance

    def to_flyte_idl(self) -> tasks_pb2.GPUAccelerator:
        msg = tasks_pb2.GPUAccelerator(device=self._device)
        if not hasattr(self, "_partition_size"):
            return msg

        if self._partition_size is None:
            msg.unpartitioned = True
        else:
            msg.partition_size = self._partition_size.value
        return msg


class _NvidiaTeslaA100_PartitionSizes(enum.Enum):
    PARTITION_1G_5GB = "1g.5gb"
    PARTITION_2G_10GB = "2g.10gb"
    PARTITION_3G_20GB = "3g.20gb"
    PARTITION_7G_40GB = "7g.40gb"


NvidiaTeslaA100 = MultiInstanceGPUAccelerator("nvidia-tesla-a100", _NvidiaTeslaA100_PartitionSizes)


class _NvidiaTeslaA100_80GB_PartitionSizes(enum.Enum):
    PARTITION_1G_10GB = "1g.10gb"
    PARTITION_2G_20GB = "2g.20gb"
    PARTITION_3G_40GB = "3g.40gb"
    PARTITION_7G_80GB = "7g.80gb"


NvidiaTeslaA100_80GB = MultiInstanceGPUAccelerator("nvidia-a100-80gb", _NvidiaTeslaA100_80GB_PartitionSizes)
