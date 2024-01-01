import abc
import copy
from typing import ClassVar, Generic, Optional, Type, TypeVar

from flyteidl.core import tasks_pb2

T = TypeVar("T")
MIG = TypeVar("MIG", bound="MultiInstanceGPUAccelerator")


class BaseAccelerator(abc.ABC, Generic[T]):
    """
    Base class for all accelerator types. This class is not meant to be instantiated directly.
    """

    @abc.abstractmethod
    def to_flyte_idl(self) -> T:
        ...


class GPUAccelerator(BaseAccelerator):
    """
    Class that represents a GPU accelerator. The class can be instantiated with any valid GPU device name, but
    it is recommended to use one of the pre-defined constants below, as name has to match the name of the device
    configured on the cluster.
    """

    def __init__(self, device: str) -> None:
        self._device = device

    def to_flyte_idl(self) -> tasks_pb2.GPUAccelerator:
        return tasks_pb2.GPUAccelerator(device=self._device)


#: use this constant to specify that the task should run on an
#: `NVIDIA A10 Tensor Core GPU<https://www.nvidia.com/en-us/data-center/a10-tensor-core-gpu/>`_
A10G = GPUAccelerator("nvidia-a10g")

#: use this constant to specify that the task should run on an
#: `NVIDIA L4 Tensor Core GPU<https://www.nvidia.com/en-us/data-center/l4/>`_
L4 = GPUAccelerator("nvidia-l4-vws")

#: use this constant to specify that the task should run on an
#: `NVIDIA Tesla K80 GPU<https://www.nvidia.com/en-gb/data-center/tesla-k80/>`_
K80 = GPUAccelerator("nvidia-tesla-k80")

#: use this constant to specify that the task should run on an
#: `NVIDIA Tesla M60 GPU<https://www.nvidia.com/en-us/data-center/tesla-m60/>`_
M60 = GPUAccelerator("nvidia-tesla-m60")

#: use this constant to specify that the task should run on an
#: `NVIDIA Tesla P4 GPU<https://www.nvidia.com/en-us/data-center/tesla-p4/>`_
P4 = GPUAccelerator("nvidia-tesla-p4")

#: use this constant to specify that the task should run on an
#: `NVIDIA Tesla P100 GPU<https://www.nvidia.com/en-us/data-center/tesla-p100/>`_
P100 = GPUAccelerator("nvidia-tesla-p100")

#: use this constant to specify that the task should run on an
#: `NVIDIA T4 Tensor Core GPU<https://www.nvidia.com/en-us/data-center/t4/>`_
T4 = GPUAccelerator("nvidia-tesla-t4")

#: use this constant to specify that the task should run on an
#: `NVIDIA Tesla V100 GPU<https://www.nvidia.com/en-us/data-center/tesla-v100/>`_
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
    """
    Class that represents an `NVIDIA A100 GPU<https://www.nvidia.com/en-us/data-center/a100/>`_. It is possible
    to specify a partition of an A100 GPU by using the provided paritions on the class. For example, to specify a
    10GB partition, use ``A100.partition_2g_10gb``.
    """

    partition_1g_5gb = _A100_Base.partitioned("1g.5gb")
    partition_2g_10gb = _A100_Base.partitioned("2g.10gb")
    partition_3g_20gb = _A100_Base.partitioned("3g.20gb")
    partition_4g_20gb = _A100_Base.partitioned("4g.20gb")
    partition_7g_40gb = _A100_Base.partitioned("7g.40gb")


#: use this constant to specify that the task should run on an entire
#: `NVIDIA A100 GPU<https://www.nvidia.com/en-us/data-center/a100/>`_.
#: It is also possible to specify a partition of an A100 GPU by using the provided paritions on the class.
#: For example, to specify a 10GB partition, use ``A100.partition_2g_10gb``.
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
