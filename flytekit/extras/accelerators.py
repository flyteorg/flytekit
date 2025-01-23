"""
Specifying Accelerators
==========================

.. tags:: MachineLearning, Advanced, Hardware

Flyte allows you to specify `gpu` resources for a given task. However, in some cases, you may want to use a different
accelerator type, such as TPU, specific variations of GPUs, or fractional GPUs. You can configure the Flyte backend to
use your preferred accelerators, and those who write workflow code can import the `flytekit.extras.accelerators` module
to specify an accelerator in the task decorator.


If you want to use a specific GPU device, you can pass the device name directly to the task decorator, e.g.:

.. code-block::

    @task(
        limits=Resources(gpu="1"),
        accelerator=GPUAccelerator("nvidia-tesla-v100"),
    )
    def my_task() -> None:
        ...


Base Classes
------------
These classes can be used to create custom accelerator type constants. For example, you can create a TPU accelerator.



.. currentmodule:: flytekit.extras.accelerators

.. autosummary::
   :template: custom.rst
   :toctree: generated/
   :nosignatures:

   BaseAccelerator
   GPUAccelerator
   MultiInstanceGPUAccelerator

But, often, you may want to use a well known accelerator type, and to simplify this, flytekit provides a set of
predefined accelerator constants, as described in the next section.


Predefined Accelerator Constants
--------------------------------

The `flytekit.extras.accelerators` module provides some constants for known accelerators, listed below, but this is not
a complete list. If you know the name of the accelerator, you can pass the string name to the task decorator directly.

If using the constants, you can import them directly from the module, e.g.:

.. code-block::

    from flytekit.extras.accelerators import T4

    @task(
        limits=Resources(gpu="1"),
        accelerator=T4,
    )
    def my_task() -> None:
        ...

if you want to use a fractional GPU, you can use the ``partitioned`` method on the accelerator constant, e.g.:

.. code-block::

    from flytekit.extras.accelerators import A100

    @task(
        limits=Resources(gpu="1"),
        accelerator=A100.partition_2g_10gb,
    )
    def my_task() -> None:
        ...

.. currentmodule:: flytekit.extras.accelerators

.. autosummary::
   :toctree: generated/
   :nosignatures:

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
    def to_flyte_idl(self) -> T: ...


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
#: `NVIDIA A10 Tensor Core GPU <https://www.nvidia.com/en-us/data-center/products/a10-gpu/>`_
A10G = GPUAccelerator("nvidia-a10g")

#: use this constant to specify that the task should run on an
#: `NVIDIA L4 Tensor Core GPU <https://www.nvidia.com/en-us/data-center/l4/>`_
L4 = GPUAccelerator("nvidia-l4")

#: use this constant to specify that the task should run on an
#: `NVIDIA L4 Tensor Core GPU <https://www.nvidia.com/en-us/data-center/l4/>`_
L4_VWS = GPUAccelerator("nvidia-l4-vws")

#: use this constant to specify that the task should run on an
#: `NVIDIA Tesla K80 GPU <https://www.nvidia.com/en-gb/data-center/tesla-k80/>`_
K80 = GPUAccelerator("nvidia-tesla-k80")

#: use this constant to specify that the task should run on an
#: `NVIDIA Tesla M60 GPU <https://images.nvidia.com/content/tesla/pdf/188417-Tesla-M60-DS-A4-fnl-Web.pdf/>`_
M60 = GPUAccelerator("nvidia-tesla-m60")

#: use this constant to specify that the task should run on an
#: `NVIDIA Tesla P4 GPU <https://images.nvidia.com/content/pdf/tesla/184457-Tesla-P4-Datasheet-NV-Final-Letter-Web.pdf/>`_
P4 = GPUAccelerator("nvidia-tesla-p4")

#: use this constant to specify that the task should run on an
#: `NVIDIA Tesla P100 GPU <https://images.nvidia.com/content/tesla/pdf/nvidia-tesla-p100-datasheet.pdf/>`_
P100 = GPUAccelerator("nvidia-tesla-p100")

#: use this constant to specify that the task should run on an
#: `NVIDIA Tesla T4 GPU <https://www.nvidia.com/en-us/data-center/tesla-t4/>`_
T4 = GPUAccelerator("nvidia-tesla-t4")

#: use this constant to specify that the task should run on an
#: `NVIDIA Tesla V100 GPU <https://images.nvidia.com/content/technologies/volta/pdf/tesla-volta-v100-datasheet-letter-fnl-web.pdf/>`_
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
    to specify a partition of an A100 GPU by using the provided partitions on the class. For example, to specify a
    10GB partition, use ``A100.partition_2g_10gb``.
    Refer to `Partitioned GPUs <https://docs.nvidia.com/datacenter/tesla/mig-user-guide/index.html#partitioning>`_
    """

    partition_1g_5gb = _A100_Base.partitioned("1g.5gb")
    """
    5GB partition of an A100 GPU.
    """
    partition_2g_10gb = _A100_Base.partitioned("2g.10gb")
    """
    10GB partition of an A100 GPU - 2x5GB slices with 2/7th of the SM.
    """
    partition_3g_20gb = _A100_Base.partitioned("3g.20gb")
    """
    20GB partition of an A100 GPU - 4x5GB slices, with 3/7th fraction of SM (Streaming multiprocessor).
    """
    partition_4g_20gb = _A100_Base.partitioned("4g.20gb")
    """
    20GB partition of an A100 GPU - 4x5GB slices, with 4/7th fraction of SM.
    """
    partition_7g_40gb = _A100_Base.partitioned("7g.40gb")
    """
    40GB partition of an A100 GPU - 8x5GB slices, with 7/7th fraction of SM.
    """


#: Use this constant to specify that the task should run on an entire
#: `NVIDIA A100 GPU <https://www.nvidia.com/en-us/data-center/a100/>`_. Fractional partitions are also available.
#:
#: Use pre-defined partitions (as instance attributes). For example, to specify a 10GB partition, use
#: ``A100.partition_2g_10gb``.
#: All partitions are nested in the class as follows:
#:
#: .. autoclass:: _A100
#:    :members:
A100 = _A100()


class _A100_80GB_Base(MultiInstanceGPUAccelerator):
    device = "nvidia-a100-80gb"


class _A100_80GB(_A100_80GB_Base):
    """
    Partitions of an `NVIDIA A100 80GB GPU <https://www.nvidia.com/en-us/data-center/a100/>`_.
    """

    partition_1g_10gb = _A100_80GB_Base.partitioned("1g.10gb")
    """
    10GB partition of an A100 80GB GPU - 2x5GB slices with 1/7th of the SM.
    """
    partition_2g_20gb = _A100_80GB_Base.partitioned("2g.20gb")
    """
    2GB partition of an A100 80GB GPU - 4x5GB slices with 2/7th of the SM.
    """
    partition_3g_40gb = _A100_80GB_Base.partitioned("3g.40gb")
    """
    3GB partition of an A100 80GB GPU - 8x5GB slices with 3/7th of the SM.
    """
    partition_4g_40gb = _A100_80GB_Base.partitioned("4g.40gb")
    """
    4GB partition of an A100 80GB GPU - 8x5GB slices with 4/7th of the SM.
    """
    partition_7g_80gb = _A100_80GB_Base.partitioned("7g.80gb")
    """
    7GB partition of an A100 80GB GPU - 16x5GB slices with 7/7th of the SM.
    """


#: use this constant to specify that the task should run on an entire
#: `NVIDIA A100 80GB GPU <https://www.nvidia.com/en-us/data-center/a100/>`_. Fractional partitions are also available.
#:
#: Use pre-defined partitions (as instance attributes). For example, to specify a 10GB partition, use
#: ``A100.partition_2g_10gb``.
#: All available partitions are listed below:
#:
#: .. autoclass:: _A100_80GB
#:    :members:
A100_80GB = _A100_80GB()


class _V5E_Base(MultiInstanceGPUAccelerator):
    device = "tpu-v5-lite-podslice"


class _V5E(_V5E_Base):
    """
    Slices of a `Google Cloud TPU v5e <https://cloud.google.com/tpu/docs/v5e>`_.
    """

    slice_1x1 = _V5E_Base.partitioned("1x1")
    """
    1x1 topology representing 1 TPU chip or 1/8 of a host.
    """
    slice_2x2 = _V5E_Base.partitioned("2x2")
    """
    2x2 topology representing 4 TPU chip or 1/2 of a host.
    """
    slice_2x4 = _V5E_Base.partitioned("2x4")
    """
    2x4 topology representing 8 TPU chip or 1 host.
    """
    slice_4x4 = _V5E_Base.partitioned("4x4")
    """
    4x4 topology representing 16 TPU chip or 2 hosts.
    """
    slice_4x8 = _V5E_Base.partitioned("4x8")
    """
    4x8 topology representing 32 TPU chip or 4 hosts.
    """
    slice_8x8 = _V5E_Base.partitioned("8x8")
    """
    8x8 topology representing 64 TPU chip or 8 hosts.
    """
    slice_8x16 = _V5E_Base.partitioned("8x16")
    """
    8x16 topology representing 128 TPU chip or 16 hosts.
    """
    slice_16x16 = _V5E_Base.partitioned("16x16")
    """
    16x16 topology representing 256 TPU chip or 32 hosts.
    """


#: use this constant to specify that the task should run on V5E TPU.
#: `Google V5E Cloud TPU <https://cloud.google.com/tpu/docs/v5e>`_.
#:
#: Use pre-defined slices (as instance attributes). For example, to specify a 2x4 slice, use
#: ``V5E.slice_2x4``.
#: All available partitions are listed below:
#:
#: .. autoclass:: _V5E
#:    :members:
V5E = _V5E()


class _V5P_Base(MultiInstanceGPUAccelerator):
    device = "tpu-v5p-slice"


class _V5P(_V5P_Base):
    """
    Slices of a `Google Cloud TPU v5p <https://cloud.google.com/tpu/docs/v5p>`_.
    """

    slice_2x2x1 = _V5P_Base.partitioned("2x2x1")
    """
    2x2x1 topology representing 8 TPU cores, 4 chips, 1 host.
    """

    slice_2x2x2 = _V5P_Base.partitioned("2x2x2")
    """
    2x2x2 topology representing 16 TPU cores, 8 chips, 2 machines.
    """

    slice_2x4x4 = _V5P_Base.partitioned("2x4x4")
    """
    2x4x4 topology representing 64 TPU cores, 32 chips, 8 machines.
    """

    slice_4x4x4 = _V5P_Base.partitioned("4x4x4")
    """
    4x4x4 topology representing 128 TPU cores, 64 chips, 16 machines.
    """

    slice_4x4x8 = _V5P_Base.partitioned("4x4x8")
    """
    4x4x8 topology representing 256 TPU cores, 128 chips, 32 machines. Supports Twisted Topology.
    """

    slice_4x8x8 = _V5P_Base.partitioned("4x8x8")
    """
    4x8x8 topology representing 512 TPU cores, 256 chips, 64 machines. Supports Twisted Topology.
    """

    slice_8x8x8 = _V5P_Base.partitioned("8x8x8")
    """
    8x8x8 topology representing 1024 TPU cores, 512 chips, 128 machines.
    """

    slice_8x8x16 = _V5P_Base.partitioned("8x8x16")
    """
    8x8x16 topology representing 2048 TPU cores, 1024 chips, 256 machines. Supports Twisted Topology.
    """

    slice_8x16x16 = _V5P_Base.partitioned("8x16x16")
    """
    8x16x16 topology representing 4096 TPU cores, 2048 chips, 512 machines. Supports Twisted Topology.
    """

    slice_16x16x16 = _V5P_Base.partitioned("16x16x16")
    """
    16x16x16 topology representing 8192 TPU cores, 4096 chips, 1024 machines.
    """

    slice_16x16x24 = _V5P_Base.partitioned("16x16x24")
    """
    16x16x24 topology representing 12288 TPU cores, 6144 chips, 1536 machines.
    """


#: Use this constant to specify that the task should run on V5P TPU.
#: `Google V5P Cloud TPU <https://cloud.google.com/tpu/docs/v5p>`_.
#:
#: Use pre-defined slices (as instance attributes). For example, to specify a 2x4x4 slice, use
#: ``V5P.slice_2x4x4``.
#: All available partitions are listed below:
#:
#: .. autoclass:: _V5P
#:    :members:
V5P = _V5P()


class _V6E_Base(MultiInstanceGPUAccelerator):
    device = "tpu-v6e-slice"


class _V6E(_V6E_Base):
    """
    Slices of a `Google Cloud TPU v6e <https://cloud.google.com/tpu/docs/v6e>`_.
    """

    slice_1x1 = _V6E_Base.partitioned("1x1")
    """
    1x1 topology representing 1 TPU core or 1/8 of a host.
    """

    slice_2x2 = _V6E_Base.partitioned("2x2")
    """
    2x2 topology representing 4 TPU cores or 1/2 of a host.
    """

    slice_2x4 = _V6E_Base.partitioned("2x4")
    """
    2x4 topology representing 8 TPU cores or 1 host.
    """

    slice_4x4 = _V6E_Base.partitioned("4x4")
    """
    4x4 topology representing 16 TPU cores or 2 hosts.
    """

    slice_4x8 = _V6E_Base.partitioned("4x8")
    """
    4x8 topology representing 32 TPU cores or 4 hosts.
    """

    slice_8x8 = _V6E_Base.partitioned("8x8")
    """
    8x8 topology representing 64 TPU cores or 8 hosts.
    """

    slice_8x16 = _V6E_Base.partitioned("8x16")
    """
    8x16 topology representing 128 TPU cores or 16 hosts.
    """

    slice_16x16 = _V6E_Base.partitioned("16x16")
    """
    16x16 topology representing 256 TPU cores or 32 hosts.
    """


#: Use this constant to specify that the task should run on V6E TPU.
#: `Google V6E Cloud TPU <https://cloud.google.com/tpu/docs/v6e>`_.
#:
#: Use pre-defined slices (as instance attributes). For example, to specify a 2x4 slice, use
#: ``V6E.slice_2x4``.
#: All available partitions are listed below:
#:
#: .. autoclass:: _V6E
#:    :members:
V6E = _V6E()
