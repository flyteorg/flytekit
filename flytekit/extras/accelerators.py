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


A10G = GPUAccelerator("nvidia-a10g")
L4 = GPUAccelerator("nvidia-l4-vws")
K80 = GPUAccelerator("nvidia-tesla-k80")
M60 = GPUAccelerator("nvidia-tesla-m60")
P4 = GPUAccelerator("nvidia-tesla-p4")
P100 = GPUAccelerator("nvidia-tesla-p100")
T4 = GPUAccelerator("nvidia-tesla-t4")
V100 = GPUAccelerator("nvidia-tesla-v100")


class MultiInstanceGPUAccelerator(BaseAccelerator, Generic[PART]):
    _partition: Optional[PART]

    def __init__(self, device: str, partitions: Type[PART]) -> None:
        self._device = device
        self._partitions = partitions

    @property
    def partitions(self) -> Type[PART]:
        return self._partitions

    def to_flyte_idl(self) -> tasks_pb2.GPUAccelerator:
        msg = tasks_pb2.GPUAccelerator(device=self._device)
        if not hasattr(self, "_partition"):
            return msg

        if self._partition is None:
            msg.unpartitioned = True
        else:
            msg.partition_size = self._partition.value
        return msg

    @property
    def unpartitioned(self: MIG) -> MIG:
        instance = copy.deepcopy(self)
        instance._partition = None
        return instance

    def partitioned(self: MIG, partition: PART) -> MIG:
        if partition not in self._partitions:
            raise ValueError(
                f"Invalid partition size for device {self._device!r}. Expected one of ({', '.join(map(str, self._partitions))}), but got: {partition}"
            )
        instance = copy.deepcopy(self)
        instance._partition = partition
        return instance


class _A100_Partitions(enum.Enum):
    PARTITION_1G_5GB = "1g.5gb"
    PARTITION_2G_10GB = "2g.10gb"
    PARTITION_3G_20GB = "3g.20gb"
    PARTITION_4G_20GB = "4g.20gb"
    PARTITION_7G_40GB = "7g.40gb"


A100 = MultiInstanceGPUAccelerator("nvidia-tesla-a100", _A100_Partitions)


class _A100_80GB_Partitions(enum.Enum):
    PARTITION_1G_10GB = "1g.10gb"
    PARTITION_2G_20GB = "2g.20gb"
    PARTITION_3G_40GB = "3g.40gb"
    PARTITION_4G_40GB = "4g.40gb"
    PARTITION_7G_80GB = "7g.80gb"


A100_80GB = MultiInstanceGPUAccelerator("nvidia-a100-80gb", _A100_80GB_Partitions)
