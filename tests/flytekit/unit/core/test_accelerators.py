from collections import OrderedDict

from flytekit.configuration import Image, ImageConfig, SerializationSettings
from flytekit.core.accelerators import NvidiaTeslaA100, NvidiaTeslaT4
from flytekit.core.task import task
from flytekit.tools.translator import get_serializable

serialization_settings = SerializationSettings(
    project="proj",
    domain="dom",
    version="123",
    image_config=ImageConfig(Image(name="name", fqn="asdf/fdsa", tag="123")),
    env={},
)


class TestAccelerators:
    def test_gpu_accelerator(self):
        @task(accelerator=NvidiaTeslaT4)
        def needs_t4(a: int):
            pass

        ts = get_serializable(OrderedDict(), serialization_settings, needs_t4).to_flyte_idl()
        gpu_accelerator = ts.template.metadata.resource_metadata.gpu_accelerator
        assert gpu_accelerator is not None
        assert gpu_accelerator.device == "nvidia-tesla-t4"
        assert not gpu_accelerator.HasField("unpartitioned")
        assert not gpu_accelerator.HasField("partition_size")

    def test_mig(self):
        @task(accelerator=NvidiaTeslaA100)
        def needs_a100(a: int):
            pass

        ts = get_serializable(OrderedDict(), serialization_settings, needs_a100).to_flyte_idl()
        gpu_accelerator = ts.template.metadata.resource_metadata.gpu_accelerator
        assert gpu_accelerator is not None
        assert gpu_accelerator.device == "nvidia-tesla-a100"
        assert not gpu_accelerator.HasField("unpartitioned")
        assert not gpu_accelerator.HasField("partition_size")

    def test_mig_unpartitioned(self):
        @task(accelerator=NvidiaTeslaA100.with_partition_size(None))
        def needs_unpartitioned_a100(a: int):
            pass

        ts = get_serializable(OrderedDict(), serialization_settings, needs_unpartitioned_a100).to_flyte_idl()
        gpu_accelerator = ts.template.metadata.resource_metadata.gpu_accelerator
        assert gpu_accelerator is not None
        assert gpu_accelerator.device == "nvidia-tesla-a100"
        assert gpu_accelerator.unpartitioned
        assert not gpu_accelerator.HasField("partition_size")

    def test_mig_partitioned(self):
        @task(accelerator=NvidiaTeslaA100.with_partition_size(NvidiaTeslaA100.partition_sizes.PARTITION_1G_5GB))
        def needs_partitioned_a100(a: int):
            pass

        ts = get_serializable(OrderedDict(), serialization_settings, needs_partitioned_a100).to_flyte_idl()
        gpu_accelerator = ts.template.metadata.resource_metadata.gpu_accelerator
        assert gpu_accelerator is not None
        assert gpu_accelerator.device == "nvidia-tesla-a100"
        assert gpu_accelerator.partition_size == "1g.5gb"
        assert not gpu_accelerator.HasField("unpartitioned")
