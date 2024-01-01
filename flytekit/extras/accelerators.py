"""
Use specific accelerators
==========================

.. tags:: MachineLearning, Advanced, Hardware

Flyte tasks are very powerful and allow you to select `gpu` resources for your task. However, there are some cases where
you may want to use a different accelerator type, such as a TPU or specific variations of GPUs or even use fractional GPU's.
Flyte makes it possible to configure the backend to utilize different accelerators and this module provides a way for
the user to request for these specific accelerators. The module provides some constant for known accelerators, but
remember this is not a complete list. If you know the name of the accelerator you want to use, you can simply pass the
string name to the task.

.. code-block::

    from flytekit.extras.accelerators import T4

    @task(
        limits=Resources(gpu="1"),
        accelerator=T4,
    )
    def my_task() -> None:
        ...


.. currentmodule:: flytekit.extras.accelerators


.. autosummary::
   :toctree: generated/

   BaseAccelerator
   GPUAccelerator
   MultiInstanceGPUAccelerator
   A10G
   L4
   K80
   M60
   P4
   P100
   T4
   V100
   A100
   A100_80GB

"""
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
#: `NVIDIA A10 Tensor Core GPU <https://www.nvidia.com/en-us/data-center/a10-tensor-core-gpu/>`_
A10G = GPUAccelerator("nvidia-a10g")

#: use this constant to specify that the task should run on an
#: `NVIDIA L4 Tensor Core GPU <https://www.nvidia.com/en-us/data-center/l4/>`_
L4 = GPUAccelerator("nvidia-l4-vws")

#: use this constant to specify that the task should run on an
#: `NVIDIA Tesla K80 GPU <https://www.nvidia.com/en-gb/data-center/tesla-k80/>`_
K80 = GPUAccelerator("nvidia-tesla-k80")

#: use this constant to specify that the task should run on an
#: `NVIDIA Tesla M60 GPU <https://www.nvidia.com/en-us/data-center/tesla-m60/>`_
M60 = GPUAccelerator("nvidia-tesla-m60")

#: use this constant to specify that the task should run on an
#: `NVIDIA Tesla P4 GPU <https://www.nvidia.com/en-us/data-center/tesla-p4/>`_
P4 = GPUAccelerator("nvidia-tesla-p4")

#: use this constant to specify that the task should run on an
#: `NVIDIA Tesla P100 GPU <https://www.nvidia.com/en-us/data-center/tesla-p100/>`_
P100 = GPUAccelerator("nvidia-tesla-p100")

#: use this constant to specify that the task should run on an
#: `NVIDIA T4 Tensor Core GPU <https://www.nvidia.com/en-us/data-center/t4/>`_
T4 = GPUAccelerator("nvidia-tesla-t4")

#: use this constant to specify that the task should run on an
#: `NVIDIA Tesla V100 GPU <https://www.nvidia.com/en-us/data-center/tesla-v100/>`_
V100 = GPUAccelerator("nvidia-tesla-v100")


class MultiInstanceGPUAccelerator(BaseAccelerator):
    """
    Base class for all multi-instance GPU accelerator types. It is recommended to use one of the pre-defined constants
    below, as name has to match the name of the device configured on the cluster.
    For example, to specify a 10GB partition of an A100 GPU, use ``A100.partition_2g_10gb``.
    """

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
    Class that represents an `NVIDIA A100 GPU <https://www.nvidia.com/en-us/data-center/a100/>`_. It is possible
    to specify a partition of an A100 GPU by using the provided paritions on the class. For example, to specify a
    10GB partition, use ``A100.partition_2g_10gb``.
    """

    partition_1g_5gb = _A100_Base.partitioned("1g.5gb")
    """
    1GB partition of an A100 GPU.
    """
    partition_2g_10gb = _A100_Base.partitioned("2g.10gb")
    """
    2GB partition of an A100 GPU.
    """
    partition_3g_20gb = _A100_Base.partitioned("3g.20gb")
    """
    3GB partition of an A100 GPU.
    """
    partition_4g_20gb = _A100_Base.partitioned("4g.20gb")
    """
    4GB partition of an A100 GPU.
    """
    partition_7g_40gb = _A100_Base.partitioned("7g.40gb")
    """
    7GB partition of an A100 GPU.
    """


#: use this constant to specify that the task should run on an entire
#: `NVIDIA A100 GPU <https://www.nvidia.com/en-us/data-center/a100/>`_.
#: It is also possible to specify a partition of an A100 GPU by using the provided partitions on the class.
#: For example, to specify a 10GB partition, use ``A100.partition_2g_10gb``.
#: All partitions are listed in :py:class:`flytekit.extras.accelerators._A100`.
A100 = _A100()


class _A100_80GB_Base(MultiInstanceGPUAccelerator):
    device = "nvidia-a100-80gb"


class _A100_80GB(_A100_80GB_Base):
    """
    Partitions of an `NVIDIA A100 80GB GPU <https://www.nvidia.com/en-us/data-center/a100/>`_.
    """

    partition_1g_10gb = _A100_80GB_Base.partitioned("1g.10gb")
    """
    1GB partition of an A100 80GB GPU.
    """
    partition_2g_20gb = _A100_80GB_Base.partitioned("2g.20gb")
    """
    2GB partition of an A100 80GB GPU.
    """
    partition_3g_40gb = _A100_80GB_Base.partitioned("3g.40gb")
    """
    3GB partition of an A100 80GB GPU.
    """
    partition_4g_40gb = _A100_80GB_Base.partitioned("4g.40gb")
    """
    4GB partition of an A100 80GB GPU.
    """
    partition_7g_80gb = _A100_80GB_Base.partitioned("7g.80gb")
    """
    7GB partition of an A100 80GB GPU.
    """


#: use this constant to specify that the task should run on an entire
#: `NVIDIA A100 80GB GPU <https://www.nvidia.com/en-us/data-center/a100/>`_.
#: It is also possible to specify a partition of an A100 GPU by using the provided partitions on the class.
#: For example, to specify a 10GB partition, use ``A100.partition_2g_10gb``.
#: All partitions are listed in :py:class:`flytekit.extras.accelerators._A100_80GB`.
A100_80GB = _A100_80GB()
