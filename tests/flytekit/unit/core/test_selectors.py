from collections import OrderedDict

from flyteidl.core.tasks_pb2 import Selector

from flytekit.configuration import Image, ImageConfig, SerializationSettings
from flytekit.core.context_manager import FlyteContextManager
from flytekit.core.data_persistence import flyte_tmp_dir
from flytekit.core.selectors import GPUDeviceA100, GPUPartition2G10GB, GPUUnpartitioned
from flytekit.core.task import task
from flytekit.tools.translator import get_serializable

serialization_settings = SerializationSettings(
    project="proj",
    domain="dom",
    version="123",
    image_config=ImageConfig(Image(name="name", fqn="asdf/fdsa", tag="123")),
    env={},
)


class TestSelectors:
    def test_gpu_device_selector(self):
        @task(selectors=[GPUDeviceA100])
        def needs_a100(a: int):
            ctx = FlyteContextManager.current_context()
            wf_params = ctx.user_space_params
            assert str(wf_params.execution_id) == "ex:local:local:local"
            assert flyte_tmp_dir in wf_params.raw_output_prefix

        ts = get_serializable(OrderedDict(), serialization_settings, needs_a100)
        assert len(ts.template.metadata.selectors) == 1
        s = ts.template.metadata.selectors[0]
        assert isinstance(s, Selector)
        assert s.HasField("gpu_device")
        assert s.gpu_device == "nvidia-tesla-a100"

    def test_gpu_unpartitioned(self):
        @task(selectors=[GPUDeviceA100, GPUUnpartitioned])
        def needs_unpartitioned_a100(a: int):
            ctx = FlyteContextManager.current_context()
            wf_params = ctx.user_space_params
            assert str(wf_params.execution_id) == "ex:local:local:local"
            assert flyte_tmp_dir in wf_params.raw_output_prefix

        ts = get_serializable(OrderedDict(), serialization_settings, needs_unpartitioned_a100)
        assert len(ts.template.metadata.selectors) == 2
        s1 = ts.template.metadata.selectors[0]
        assert s1.HasField("gpu_device")
        assert s1.gpu_device == "nvidia-tesla-a100"
        s2 = ts.template.metadata.selectors[1]
        assert s2.HasField("gpu_unpartitioned")
        assert s2.gpu_unpartitioned

    def test_gpu_partitioned(self):
        @task(selectors=[GPUDeviceA100, GPUPartition2G10GB])
        def needs_partitioned_a100(a: int):
            ctx = FlyteContextManager.current_context()
            wf_params = ctx.user_space_params
            assert str(wf_params.execution_id) == "ex:local:local:local"
            assert flyte_tmp_dir in wf_params.raw_output_prefix

        ts = get_serializable(OrderedDict(), serialization_settings, needs_partitioned_a100)
        assert len(ts.template.metadata.selectors) == 2
        s1 = ts.template.metadata.selectors[0]
        assert s1.HasField("gpu_device")
        assert s1.gpu_device == "nvidia-tesla-a100"
        s2 = ts.template.metadata.selectors[1]
        assert s2.HasField("gpu_partition_size")
        assert s2.gpu_partition_size == "2g.10gb"
