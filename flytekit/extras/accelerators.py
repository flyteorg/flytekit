import abc
import copy
from typing import ClassVar, Generic, Optional, Type, TypeVar

from flyteidl.core import tasks_pb2

T = TypeVar("T")
MIG = TypeVar("MIG", bound="MultiInstanceGPUAccelerator")


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


class MultiInstanceGPUAccelerator(BaseAccelerator):
    device: ClassVar[str]
    _partition_size: Optional[str]

    @property
    def unpartitioned(self: MIG) -> MIG:
        instance = copy.deepcopy(self)
        instance._partition_size = None
        return instance

    @classmethod
    def partitioned(cls: Type[MIG], partition_size: str) -> MIG:
        instance = cls()
        instance._partition_size = partition_size
        return instance

    def to_flyte_idl(self) -> tasks_pb2.GPUAccelerator:
        msg = tasks_pb2.GPUAccelerator(device=self.device)
        if not hasattr(self, "_partition_size"):
            return msg

        if self._partition_size is None:
            msg.unpartitioned = True
        else:
            msg.partition_size = self._partition_size
        return msg


class _A100_Base(MultiInstanceGPUAccelerator):
    device = "nvidia-tesla-a100"


class _A100(_A100_Base):
    partition_1g_5gb = _A100_Base.partitioned("1g.5gb")
    partition_2g_10gb = _A100_Base.partitioned("2g.10gb")
    partition_3g_20gb = _A100_Base.partitioned("3g.20gb")
    partition_4g_20gb = _A100_Base.partitioned("4g.20gb")
    partition_7g_40gb = _A100_Base.partitioned("7g.40gb")


A100 = _A100()


class _A100_80GB_Base(MultiInstanceGPUAccelerator):
    device = "nvidia-a100-80gb"


class _A100_80GB(_A100_80GB_Base):
    partition_1g_10gb = _A100_80GB_Base.partitioned("1g.10gb")
    partition_2g_20gb = _A100_80GB_Base.partitioned("2g.20gb")
    partition_3g_40gb = _A100_80GB_Base.partitioned("3g.40gb")
    partition_4g_40gb = _A100_80GB_Base.partitioned("4g.40gb")
    partition_7g_80gb = _A100_80GB_Base.partitioned("7g.80gb")


A100_80GB = _A100_80GB()
